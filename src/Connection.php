<?php

namespace PNixx\Memcache;

use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\Socket\ConnectContext;
use Amp\Socket\ConnectException;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use function Amp\Socket\connect;

final class Connection {

	const WAIT = 0.2;

	private Socket $socket;
	private bool $disconnecting = false;
	private string $loop;
	private ?string $loop_readable;

	/** @var Command[] */
	private array $tasks = [];
	private string $buffer = '';
	private ?string $response = null;
	private ?int $length = null;

	/**
	 * @param string               $server
	 * @param LoggerInterface|null $logger
	 */
	public function __construct(private readonly string $server, private readonly ?LoggerInterface $logger = null) {
		$this->connect();
		$this->loop = EventLoop::repeat(1000, function() {
			if( !$this->isAlive() && !$this->disconnecting ) {
				$this->connect();
			}
		});
	}

	public function __destruct() {
		EventLoop::cancel($this->loop);
	}

	/**
	 * @return bool
	 */
	public function isAlive(): bool {
		return isset($this->socket) && !$this->socket->isClosed();
	}

	/**
	 * @return void
	 */
	public function connect(): void {
		try {
			$this->close();
			$connectContext = (new ConnectContext)->withConnectTimeout(1000);
			$this->socket = connect($this->server, $connectContext);
			$this->disconnecting = false;
			$this->loop_readable = EventLoop::onReadable($this->socket->getResource(), $this->read(...));
		} catch (ConnectException|CancelledException $e) {
			$this->logger?->info('Memcache connection error: ' . $e->getMessage());
		}
	}

	/**
	 * @param Command $command
	 * @return mixed
	 */
	public function query(Command $command): mixed {
		if( $this->isAlive() ) {
			try {
				if( $command->no_reply ) {
					$this->socket->write($command->buffer());
				} else {
					$command->deferred = new DeferredFuture();
					$this->tasks[] = $command;
					$this->socket->write($command->buffer());
					try {
						return $command->deferred->getFuture()->await(new TimeoutCancellation(self::WAIT));
					} catch (CancelledException $e) {
						$this->logger?->info('Memcache query timeout, reconnect');
						$this->socket->close();
						return null;
					}
				}
			} catch (\Throwable $e) {
				$this->logger?->info('Memcache query error: ' . $e->getMessage());
			}
		} else {
			$this->logger?->debug('Memcache ' . $this->server . ' not connected');
		}
		return null;
	}

	/**
	 * @return void
	 */
	public function close(): void {
		$this->disconnecting = true;
		if( isset($this->loop_readable) && $this->loop_readable ) {
			EventLoop::cancel($this->loop_readable);
		}
		if( $this->isAlive() ) {
			$this->socket->close();
		}
		//Если были таски, обнуляем
		foreach( $this->tasks as $command ) {
			$command->deferred->complete();
		}
		$this->tasks = [];
		$this->response = null;
		$this->length = null;
	}

	/**
	 * @throws MemcacheError
	 */
	private function read(): void {
		$chunk = $this->socket->read();
		if( $chunk !== null ) {
			$this->buffer .= $chunk;
			$this->parseRaw();
		}
	}

	/**
	 * @throws MemcacheError
	 */
	private function parseRaw(): void {
		//Повторяем до тех пор, пока есть данные
		while( $this->buffer && ($size = strpos($this->buffer, Memcache::CRLF)) !== false ) {
			//Отрезаем кусок
			$chunk = substr($this->buffer, 0, $size);
			$this->buffer = substr($this->buffer, $size + strlen(Memcache::CRLF));

			//Если есть ожидающая часть, дополняем ее
			if( $this->response !== null && $this->length >= strlen($this->response) ) {
				$this->parseAnswerValue($chunk);
				continue;
			}

			//Разбиваем строку на команды
			$line = explode(' ', $chunk);

			//Проверяем полученную команду
			switch( $line[0] ) {
				case 'VALUE':
					$this->response = '';
					$this->length = $line[3];
					if( empty($this->tasks) ) {
						$this->logger?->warning('Incorrect result. No tasks waiting.');
					} elseif( $this->tasks[0]->key != $line[1] ) {
						$this->logger?->warning('Incorrect result. Wait key `' . $this->tasks[0]->key . '`, but received `' . $line[1] . '`');
					}
					break;
				case 'VA':
					$chunk = substr($this->buffer, 0, $line[1]);
					$this->buffer = substr($this->buffer, (int)$line[1] + strlen(Memcache::CRLF));
					$this->answer($chunk);
					break;
				case 'NOT_STORED':
				case 'NS':
					$this->answer(false);
					break;
				case 'ERROR':
				case 'CLIENT_ERROR':
				case 'SERVER_ERROR':
					if( $this->tasks ) {
						$this->logger?->warning('Chunk: ' . $chunk . PHP_EOL . 'COMMAND:' . PHP_EOL . $this->tasks[0]->query);
						$this->answer(null);
					}
					break;
				case 'EXISTS':
				case 'NOT_FOUND':
				case 'NF':
					$this->answer(null);
					break;
				case 'END':
					$buffer = $this->response;
					$this->response = null;
					$this->answer($buffer);
					break;
				case 'STORED':
				case 'DELETED':
				case 'TOUCHED':
				case 'OK':
				case 'HD':
				case 'EX':
					$this->answer(true);
					break;
				default:
					throw new MemcacheError('Unknown command: ' . $line[0] . ', tasks: ' . count($this->tasks) . ', line: ' . $chunk . ', buffer: ' . $this->buffer);
			}
		}
	}

	/**
	 * @param string|null $buffer
	 */
	private function answer(?string $buffer): void {
		$command = array_shift($this->tasks);
		$command?->deferred->complete($buffer);
	}

	/**
	 * Собирает ответ и ожидает команды завершения
	 * @param string $chunks
	 * @throws MemcacheError
	 */
	private function parseAnswerValue(string $chunks): void {
		if( $chunks === 'END' ) {
			if( strlen($this->response) != $this->length ) {
				throw new MemcacheError('Incorrect body, received ' . strlen($this->response) . ' bytes. Buffer: ' . $this->response);
			}
			$buffer = $this->response;
			$this->response = null;
			$this->length = null;
			$this->answer($buffer);
		} else {
			$this->response .= $chunks;
		}
	}
}

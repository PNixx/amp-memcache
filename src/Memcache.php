<?php

namespace PNixx\Memcache;

use Psr\Log\LoggerInterface;

/**
 * @link https://github.com/memcached/memcached/blob/master/doc/protocol.txt
 */
final class Memcache {
	public const CRLF = "\r\n";

	const POINTS_PER_SERVER = 160;
	const MAX_KEY_LENGTH = 250;

	private int $pool;

	/**
	 * @var Connection[]
	 */
	private array $continuum = [];

	/**
	 * @var Connection[]
	 */
	private array $connections;

	/**
	 * @param array                $servers
	 * @param LoggerInterface|null $logger
	 */
	public function __construct(private readonly array $servers, private readonly ?LoggerInterface $logger = null) {
		$this->pool = count($servers);
		foreach( $this->servers as $server ) {
			$this->connections[] = new Connection($server, $this->logger);
		}
		$this->buildContinuum();
	}

	/**
	 * @param string $key
	 * @return string|null
	 * @throws MemcacheError
	 */
	public function get(string $key): ?string {
		$key = $this->validateKey($key);
		return $this->query(new Command('get ' . $key, $key));
	}

	/**
	 * Get And Touch
	 * @param string $key
	 * @param int    $ttl
	 * @return string|null
	 * @throws MemcacheError
	 */
	public function gat(string $key, int $ttl): ?string {
		$key = $this->validateKey($key);
		return $this->query(new Command('gat ' . $ttl . ' ' . $key, $key));
	}

	/**
	 * @param string      $key
	 * @param string $value
	 * @param int|null    $ttl
	 * @throws MemcacheError
	 */
	public function set(string $key, string $value, int $ttl = null): void {
		$key = $this->validateKey($key);
		$params = ['ms', $key, strlen($value)];
		if( $ttl > 0 ) {
			$params[] = 'T' . $ttl;
		}
		$this->query(new Command(implode(' ', $params), $key, $value));
	}

	/**
	 * Returns `true` if it was possible to write the key with the value, `false` - the key already exists
	 * @param string   $key
	 * @param string   $value
	 * @param int|null $ttl
	 * @return bool
	 * @throws MemcacheError
	 */
	public function add(string $key, string $value, int $ttl = null): bool {
		$key = $this->validateKey($key);
		$params = ['ms', $key, strlen($value), 'ME'];
		if( $ttl > 0 ) {
			$params[] = 'T' . $ttl;
		}
		return $this->query(new Command(implode(' ', $params), $key, $value));
	}

	/**
	 * Set only if item already exists
	 * @param string   $key
	 * @param string   $value
	 * @param int|null $ttl
	 * @return bool
	 * @throws MemcacheError
	 */
	public function replace(string $key, string $value, int $ttl = null): bool {
		$key = $this->validateKey($key);
		$params = ['ms', $key, strlen($value), 'MR'];
		if( $ttl > 0 ) {
			$params[] = 'T' . $ttl;
		}
		return $this->query(new Command(implode(' ', $params), $key, $value));
	}

	/**
	 * @param string $key
	 * @throws MemcacheError
	 */
	public function delete(string $key): void {
		$key = $this->validateKey($key);
		$this->query(new Command('delete ' . $key . ' noreply', $key, null, true));
	}

	/**
	 * @param string $key
	 * @param int    $ttl
	 * @return void
	 * @throws MemcacheError
	 */
	public function touch(string $key, int $ttl): void {
		$key = $this->validateKey($key);
		$this->query(new Command('touch ' . $key . ' ' . $ttl . ' noreply', $key, null, true));
	}

	/**
	 * @param string $key
	 * @param int    $offset
	 * @param int    $initial_value
	 * @param int    $ttl
	 * @return int|null
	 * @throws MemcacheError
	 */
	public function increment(string $key, int $offset = 1, int $initial_value = 0, int $ttl = 0): ?int {
		$key = $this->validateKey($key);
		$params = ['ma', $key, 'N' . $ttl, 'D' . $offset, 'J' . $initial_value, 'v'];
		if( $ttl > 0 ) {
			$params[] = 'T' . $ttl;
		}
		return $this->query(new Command(implode(' ', $params), $key));
	}

	/**
	 * @param string $key
	 * @param int    $offset
	 * @param int    $initial_value
	 * @param int    $ttl
	 * @return int|null
	 * @throws MemcacheError
	 */
	public function decrement(string $key, int $offset = 1, int $initial_value = 0, int $ttl = 0): ?int {
		$key = $this->validateKey($key);
		$params = ['ma', $key, 'D' . $offset, 'J' . $initial_value, 'MD', 'v'];
		if( $ttl > 0 ) {
			$params[] = 'N' . $ttl;
		}
		return $this->query(new Command(implode(' ', $params), $key));
	}

	/**
	 * @return void
	 */
	public function flush(): void {
		foreach( $this->connections as $connection ) {
			$connection->query(new Command('flush_all'));
		}
	}

	/**
	 * @return void
	 */
	public function close(): void {
		foreach( $this->connections as $connection ) {
			$connection->close();
		}
	}

	/**
	 * @param string $key
	 * @return string
	 * @throws MemcacheError
	 */
	private function validateKey(string $key): string {
		if( empty($key) ) {
			throw new MemcacheError('Key cannot be blank');
		}
		if( str_contains($key, ' ') ) {
			throw new MemcacheError('Space not allow in the key: `' . $key . '`');
		}
		if( !ctype_print($key) ) {
			throw new MemcacheError('Allow only printable characters for key: `' . $key . '`');
		}
		if( strlen($key) > self::MAX_KEY_LENGTH ) {
			$md5 = ':md5:' . md5($key);
			$key = substr($key, 0, self::MAX_KEY_LENGTH - strlen($md5)) . $md5;
		}
		return $key;
	}

	/**
	 * @param Command $command
	 * @return mixed
	 */
	private function query(Command $command): mixed {
		return $this->connectionForKey($command->key)?->query($command);
	}

	/**
	 * @param $key
	 * @return Connection|null
	 */
	private function connectionForKey($key): ?Connection {
		if( $this->pool <= 1 ) {
			return $this->connections[0] ?? null;
		}
		$hash_key = crc32($key);
		for( $i = 0; $i < $this->pool; $i++ ) {
			$connection = $this->connectionForHashKey($hash_key);
			if( $connection->isAlive() ) {
				return $connection;
			}
			$hash_key = crc32($i . $key);
		}
		return null;
	}

	/**
	 * https://github.com/petergoldstein/dalli/blob/7ba0f0be02ca0454ebdf346f62f1d5335f78e567/lib/dalli/ring.rb#L113-L115
	 * @return int
	 */
	private function entryCount(): int {
		$server_weight = 1;
		$total_weight = $this->pool;
		return floor(($this->pool * self::POINTS_PER_SERVER * $server_weight) / $total_weight);
	}

	/**
	 * https://github.com/petergoldstein/dalli/blob/7ba0f0be02ca0454ebdf346f62f1d5335f78e567/lib/dalli/ring.rb#L117-L126
	 * @param int $hash_key
	 * @return Connection
	 */
	private function connectionForHashKey(int $hash_key): Connection {
		$keys = array_keys($this->continuum);
		$idx = $this->binarySearchIndex($keys, $hash_key);
		if( $idx === -1 ) {
			$idx = end($keys);
		}
		return $this->continuum[$idx];
	}

	/**
	 * https://github.com/petergoldstein/dalli/blob/7ba0f0be02ca0454ebdf346f62f1d5335f78e567/lib/dalli/ring.rb#L128-L139
	 */
	private function buildContinuum(): void {
		foreach( $this->connections as $idx => $connection ) {
			for( $i = 0; $i < $this->entryCount(); $i++ ) {
				$hash = bin2hex(sha1($this->servers[$idx] . ':' . $i, true));
				$value = hexdec('0x' . substr($hash, 0, 8));
				$this->continuum[$value] = $connection;
			}
		}
		ksort($this->continuum);
	}

	/**
	 * Performs the binary search over
	 * sorted array
	 * It should perform this operation in O(logn)
	 * @param array $arr
	 * @param int   $value
	 * @return int
	 */
	private function binarySearchIndex(array $arr, int $value): int {
		$low = 0;
		$high = count($arr) - 1;
		while( $low <= $high ) {
			//calculate mid
			$mid = floor(($low + $high) / 2);
			//If value we are searching found at mid position then return it's index
			if( $value < $arr[$mid] ) {
				return $arr[$mid];
			} elseif( $value > $arr[$mid] ) {
				$low = $mid + 1;
			} else {
				$high = $mid - 1;
			}
		}
		return -1;
	}
}

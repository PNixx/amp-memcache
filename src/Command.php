<?php

namespace PNixx\Memcache;

use Amp\DeferredFuture;

final class Command {

	public DeferredFuture $deferred;

	public function __construct(public readonly string $query, public readonly ?string $key = null, public readonly ?string $value = null, public readonly bool $no_reply = false) {}

	public function buffer(): string {
		return $this->query . ($this->value !== null ? Memcache::CRLF . $this->value : '') . Memcache::CRLF;
	}
}

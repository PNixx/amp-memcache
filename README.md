# AMP Memcache

`pnixx/amp-memcache` provides non-blocking access to Memcache instances. All I/O operations are handled by the [`Amp`](https://github.com/amphp/amp) concurrency framework, so you should be familiar with the basics of it.

## Required PHP Version

- PHP 8.1+

## Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require pnixx/amp-memcache
```

## Usage

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use PNixx\Memcache\Memcache;

$memcache = new Memcache(['127.0.0.1:11211']);
$memcache->set('foo', '21');
\var_dump($memcache->increment('foo', 21));
```

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.

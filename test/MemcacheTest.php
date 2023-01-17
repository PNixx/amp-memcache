<?php

namespace PNixx\Memcache\Test;

use Amp\PHPUnit\AsyncTestCase;
use PNixx\Memcache\Memcache;
use function Amp\async;
use function Amp\Future\await;
use function Amp\Future\awaitAll;

class MemcacheTest extends AsyncTestCase {

	private Memcache $memcache;

	public function setUp(): void {
		parent::setUp();

		//Создаем необходимые данные для теста
		$this->memcache = new Memcache(['127.0.0.1:11211']);
		$this->memcache->flush();
	}

	public function testSetGet() {
		$this->memcache->set('test', 'data1', 60);
		$this->assertEquals('data1', $this->memcache->get('test'));
	}

	public function testSetGetEmpty() {
		$this->memcache->set('test', '', 60);
		$this->assertTrue('' === $this->memcache->get('test'));
	}

	public function testBigData() {
		$data1 = json_encode(range(10000, 15000));
		$data2 = json_encode(range(20000, 30000));
		$this->memcache->set('t1', $data1, 60);
		$this->memcache->set('t2', $data2, 60);
		$cached1 = $this->memcache->get('t1');
		$cached2 = $this->memcache->get('t2');
		$this->assertNotNull($cached1);
		$this->assertNotNull($cached2);
		$this->assertEquals($data1, $cached1);
		$this->assertEquals($data2, $cached2);
	}

	public function testBigBodyAsync() {
		$data1 = json_encode(range(100000, 150000));
		$data2 = json_encode(range(200000, 300000));
		$this->assertNull($this->memcache->get('t1'));
		$this->memcache->set('t1', $data1, 60);
		$this->memcache->set('t2', $data2, 60);
		$this->memcache->add('t2', $data2, 60);
		[$errors, $result] = awaitAll([
			0 => async(fn() => $this->memcache->set('t3', '1', 60)),
			1 => async(fn() => $this->memcache->get('t3')),
			2 => async(fn() => $this->memcache->get('t1')),
			3 => async(fn() => $this->memcache->set('t4', '2', 60)),
			4 => async(fn() => $this->memcache->get('t4')),
			5 => async(fn() => $this->memcache->get('t2')),
			6 => async(fn() => $this->memcache->set('t5', '3', 60)),
		]);
		foreach( $errors as $error ) {
			throw $error;
		}
		$this->assertEquals('1', $result[1]);
		$this->assertNotNull($result[2]);
		$this->assertEquals($data1, $result[2]);
		$this->assertEquals('2', $result[4]);
		$this->assertNotNull($result[5]);
		$this->assertEquals($data2, $result[5]);
	}

	public function testIncrement() {
		$this->assertEquals(10, $this->memcache->increment('test', 1, 10, 60));
		$this->assertEquals(11, $this->memcache->increment('test'));
		$this->assertEquals(12, $this->memcache->increment('test'));
		$this->assertEquals(13, $this->memcache->increment('test'));
	}

	public function testDecrement() {
		$this->assertEquals(10, $this->memcache->decrement('test', 1, 10, 60));
		$this->assertEquals(9, $this->memcache->decrement('test', 1, 10, 60));
	}

	public function testAdd() {
		$this->assertTrue($this->memcache->add('test_add', 'test', 60));
		$this->assertFalse($this->memcache->add('test_add', 'test2', 60));
	}

	public function testReplace() {
		$this->assertFalse($this->memcache->replace('test_add', 'test2', 60));
		$this->assertTrue($this->memcache->add('test_add', 'test', 60));
		$this->assertTrue($this->memcache->replace('test_add', 'test', 60));
	}

	public function testAsync() {
		[$errors, $result] = awaitAll([
			async(fn() => $this->memcache->set('t4', '2', 60)),
			async(fn() => $this->memcache->get('t4')),
		]);
		foreach( $errors as $error ) {
			throw $error;
		}
		$this->assertEquals('2', $result[1]);
	}

	public function testBigAsync() {

		$fn1 = function(): bool {
			$data = $this->memcache->get('data');
			if( !$data ) {
				$this->memcache->set('data', json_encode(true), 60);
			}
			return json_decode($this->memcache->get('data'));
		};

		$fn2 = function(): int {
			return $this->memcache->increment('test2');
		};

		$fn3 = function(): int {
			return $this->memcache->increment('test3', 1, 10, 60);
		};

		$fn4 = function(): ?array {
			$this->memcache->add('test4-add', 'data', 60);
			$this->memcache->set('test4-set', 'data-set', 60);
			return await([async(fn() => $this->memcache->get('test4-add')), async(fn() => $this->memcache->get('test4-set'))]);
		};

		for( $i = 0; $i < 10; $i++ ) {
			[$errors, $promises] = awaitAll([
				async($fn1(...)),
				async($fn2(...)),
				async($fn3(...)),
				async($fn4(...)),
				async($fn1(...)),
			]);
			foreach( $errors as $error ) {
				throw $error;
			}
			$this->assertEquals(true, $promises[0]);
			$this->assertEquals($i, $promises[1]);
			$this->assertEquals(10 + $i, $promises[2]);
			$this->assertEquals(['data', 'data-set'], $promises[3]);
			$this->assertEquals(true, $promises[4]);
		}
	}

}

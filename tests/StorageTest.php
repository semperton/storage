<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Semperton\Storage\PersistentStorage;
use Semperton\Storage\MemoryStorage;
use Semperton\Search\Criteria;
use Semperton\Storage\CollectionInterface;

final class StorageTest extends TestCase
{
	public function testMemoryStorage(): void
	{
		$storage = new MemoryStorage();
		$obj = [
			'name' => 'John',
			'age' => 22
		];

		$collection = $storage->create('users');
		$id = $collection->insertOne($obj);

		$criteria = new Criteria();
		$criteria->getFilter()->equals('_id', $id);

		$data = $collection->find($criteria)->toArray();

		$data = (array)$data[0];
		$obj['_id'] = 1;

		$this->assertEquals(1, $id);
		$this->assertSame($obj, $data);
	}

	public function testFileStorage(): void
	{
		$filepath = __DIR__ . '/' . bin2hex(random_bytes(4)) . '.db';
		$storage = new PersistentStorage($filepath);

		$collection = $storage->create('misc');

		$obj = [
			'type' => 'collection',
			'name' => 'doc',
			'label' => 'Documents'
		];

		$id = $collection->insertOne($obj);

		$data = (array)$collection->find(new Criteria($id))->first();
		$obj['_id'] = 1;

		$this->assertSame($obj, $data);

		$deleted = unlink($filepath);

		$this->assertTrue($deleted);
		$this->assertFileDoesNotExist($filepath);
	}

	public function testCreate(): void
	{
		$storage = new MemoryStorage();

		$this->assertFalse($storage->exists('user'));

		$storage->create('user');

		$this->assertTrue($storage->exists('user'));
	}

	public function testGet(): void
	{
		$storage = new MemoryStorage();

		$user = $storage->get('user');

		$this->assertInstanceOf(CollectionInterface::class, $user);

		$this->assertFalse($storage->exists('user'));
	}

	public function testExists(): void
	{
		$storage = new MemoryStorage();

		$this->assertFalse($storage->exists('misc'));

		$storage->create('misc');

		$this->assertTrue($storage->exists('misc'));
	}

	public function testDelete(): void
	{
		$storage = new MemoryStorage();

		$storage->create('user');

		$this->assertTrue($storage->exists('user'));

		$storage->delete('user');

		$this->assertFalse($storage->exists('user'));
	}

	public function testAttachStorage(): void
	{
		$filepath1 = __DIR__ . '/' . bin2hex(random_bytes(4)) . '.db';
		$filepath2 = __DIR__ . '/' . bin2hex(random_bytes(4)) . '.db';

		$storage1 = new PersistentStorage($filepath1);
		$storage2 = new PersistentStorage($filepath2);

		$data = [
			'username' => 'John'
		];

		$id = $storage2->create('user')->insertOne($data);

		$data['_id'] = $id;

		$val = $storage2->get('user')->extract($id, 'username');
		$this->assertEquals('John', $val);

		$attached = $storage1->attach($filepath2, 'delegate');

		$this->assertTrue($attached);

		$user = $storage1->get('user')->findOne($id);

		$this->assertSame($data, (array)$user);

		unlink($filepath1);
		unlink($filepath2);
	}

	public function testCollections(): void
	{
		$storage = new MemoryStorage();
		$storage->create('user');
		$storage->create('post');

		$collections = $storage->collections();

		$this->assertSame(['user', 'post'], $collections);
	}

	public function testEncryption(): void
	{
		$filepath = __DIR__ . '/' . bin2hex(random_bytes(4)) . '.db';
		$storage = new PersistentStorage($filepath, 'supersecure');

		$collection = $storage->create('misc');

		$obj = [
			'type' => 'collection',
			'name' => 'doc',
			'label' => 'I ♥ You'
		];

		$id = $collection->insertOne($obj);
		$obj['_id'] = $id;

		$this->assertSame($obj, (array)$collection->findOne($id));

		unlink($filepath);
	}
}

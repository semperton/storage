<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Semperton\Storage\MemoryStorage;
use Semperton\Search\Criteria;
use Semperton\Search\Filter;

final class CollectionTest extends TestCase
{
	protected function setUp(): void
	{
	}

	public function testValid(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->get('users');

		$this->assertFalse($collection->valid());

		$collection = $storage->create('users');

		$this->assertTrue($collection->valid());
	}

	public function testInsert(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$id = $users->insert((object)[
			'name' => 'John'
		]);

		$this->assertEquals(1, $id);

		$name = $users->getValue(1, 'name');
		$this->assertEquals('John', $name);
	}

	public function testInsertMany(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$ids = $users->insertMany([
			(object)['name' => 'John'],
			(object)['name' => 'Jane'],
			(object)['name' => 'Luke']
		]);

		$this->assertSame([1, 2, 3], $ids);

		$name = $users->getValue(2, 'name');
		$this->assertEquals('Jane', $name);
	}

	public function testGetValue(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$this->assertNull($users->getValue(1, 'name'));

		$id = $users->insert((object)['name' => 'Kate']);

		$name = $users->getValue($id, 'name');
		$this->assertEquals('Kate', $name);
	}

	public function testFind(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$user = $users->find(1);

		$this->assertNull($user);

		$id = $users->insert((object)['name' => 'John']);

		$user = $users->find($id);

		$this->assertIsObject($user);
		$this->assertEquals('John', $user->name);
	}

	public function testFindAll(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->create('users');

		$collection->insertMany([
			(object)[
				'name' => 'John',
				'age' => 22,
			],
			(object)[
				'name' => 'Jane',
				'age' => 18,
			]
		]);

		$criteria = new Criteria();
		$criteria->getFilter()->like('name', 'John');

		$users = $collection->findAll($criteria);

		$this->assertIsObject($users->first());

		$criteria2 = new Criteria();
		$criteria2->getFilter()->lowerEqual('age', 22);

		$users = $collection->findAll($criteria2);

		$this->assertEquals(2, $users->count());
	}

	public function testUpdate(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$id = $users->insert((object)[
			'name' => 'John',
			'age' => 26
		]);

		$age = $users->getValue($id, 'age');
		$this->assertEquals(26, $age);

		$status = $users->update($id, (object)['age' => 30]);
		$this->assertTrue($status);

		$age = $users->getValue($id, 'age');
		$this->assertEquals(30, $age);

		$status = $users->update(2, (object)[]);

		$this->assertFalse($status);
	}

	public function testUpdateAll(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->create('data');

		$id = $collection->insert((object)[
			'number' => 22
		]);

		$number = $collection->getValue($id, 'number');

		$this->assertEquals(22, $number);

		// system field
		$filter = (new Filter())->equal('_id', $id);

		$collection->updateAll($filter, (object)[
			'number' => 33
		]);

		$number = $collection->getValue($id, 'number');

		$this->assertEquals(33, $number);
	}

	public function testDelete(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->create('data');

		$id = $collection->insert((object)[
			'number' => 22
		]);

		$number = $collection->getValue($id, 'number');

		$this->assertEquals(22, $number);

		$status = $collection->delete($id);
		$this->assertTrue($status);

		$data = $collection->find($id);

		$this->assertNull($data);
	}

	public function testDeleteAll(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->create('data');

		$id = $collection->insert((object)[
			'number' => 22
		]);

		$number = $collection->getValue($id, 'number');

		$this->assertEquals(22, $number);

		$filter = (new Filter())->equal('_id', $id);
		$collection->deleteAll($filter);

		$data = $collection->find($id);

		$this->assertNull($data);
	}

	public function testIndex(): void
	{
		// UNIQUE constraint
		$this->expectException(PDOException::class);
		$this->expectExceptionCode(23000);

		$storage = new MemoryStorage();
		$collection = $storage->create('users');

		$res = $collection->addIndex('user.name', true);

		$this->assertTrue($res);

		$id = $collection->insert((object)[
			'user' => [
				'name' => 'John',
				'age' => 22
			]
		]);

		$this->assertEquals(1, $id);

		$id2 = $collection->insert((object)[
			'user' => [
				'name' => 'John'
			]
		]);

		$this->assertNull($id2);
	}
}

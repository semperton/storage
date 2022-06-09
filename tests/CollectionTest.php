<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Semperton\Storage\MemoryStorage;
use Semperton\Search\Criteria;

final class CollectionTest extends TestCase
{
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

		$user = $users->find(new Criteria(1))->first();

		$this->assertNull($user);

		$id = $users->insert((object)['name' => 'John']);

		$user = $users->find(new Criteria($id))->first();

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

		$users = $collection->find($criteria);

		$this->assertIsObject($users->first());

		$criteria2 = new Criteria();
		$criteria2->getFilter()->lowerEqual('age', 22);

		$users = $collection->find($criteria2);

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

		$status = $users->update(new Criteria($id), (object)['age' => 30]);
		$this->assertEquals(1, $status);

		$age = $users->getValue($id, 'age');
		$this->assertEquals(30, $age);

		$status = $users->update(new Criteria($id), (object)[]);

		$this->assertEquals(1, $status);
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

		$collection->update(new Criteria($id), (object)[
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

		$status = $collection->delete(new Criteria($id));
		$this->assertEquals(1, $status);

		$data = $collection->find(new Criteria($id))->first();

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

		$collection->delete(new Criteria($id));

		$data = $collection->find(new Criteria($id))->first();

		$this->assertNull($data);
	}

	public function testIndex(): void
	{
		// UNIQUE constraint
		$this->expectException(PDOException::class);
		$this->expectExceptionCode(23000);

		$storage = new MemoryStorage();
		$collection = $storage->create('users');

		$res = $collection->createIndex('user.name', true);

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

	public function testAssociations(): void
	{
		$this->markTestSkipped();
		// $this->doesNotPerformAssertions();

		$storage = new MemoryStorage();
		$posts = $storage->create('posts');
		$comments = $storage->create('comments');

		$posts->insertMany([
			(object)[
				'title' => 'The Storage News',
				'number' => 22
			],
			(object)[
				'title' => 'New Books Avail',
				'number' => 55
			]
		]);

		$comments->insertMany([
			(object)[
				'posts_id' => 1,
				'title' => 'Good job',
				'content' => ''
			],
			(object)[
				'posts_id' => 1,
				'title' => 'Nice read',
				'content' => ''
			]
		]);

		$criteria = new Criteria(1);
		$criteria = $criteria->withField('title');

		$criteria = $criteria->withAssociation('comments', (new Criteria())->withField('title'));

		$data = $posts->find($criteria)->toArray();

		// var_dump($data);
	}

	public function testRaw(): void
	{
		$this->markTestSkipped();

		$storage = new MemoryStorage();
		$posts = $storage->create('posts');

		$id = $posts->insert([
			'title' => 'New Post',
			'views' => 22
		]);

		$rawSingle = $posts->findOneRaw($id);

		$rawAll = $posts->findRaw(new Criteria($id));

		foreach ($rawAll as $entry) {
			var_dump($entry);
		}

		$first = $rawAll->first();

		var_dump($first);
	}

	public function testDataTypes(): void
	{
		$storage = new MemoryStorage();
		$posts = $storage->create('posts');

		$data = [
			'title' => 'New Post',
			'views' => 22,
			'rating' => 1.7
		];

		$id = $posts->insert($data);
		$data['_id'] = $id;

		$criteria = new Criteria();
		$criteria->getFilter()->equals('views', 22);

		$post = $posts->find($criteria)->first();

		$this->assertSame($data, (array)$post);

		$criteria->getFilter()->reset()->greaterEqual('rating', 1.7);

		$post = $posts->find($criteria)->first();

		$this->assertSame($data, (array)$post);
	}

	public function testNullValue(): void
	{
		$storage = new MemoryStorage();
		$posts = $storage->create('user');

		$data = [
			'name' => 'John',
			'age' => null
		];

		$id = $posts->insert($data);
		$data['_id'] = $id;

		$criteria = new Criteria();
		$criteria->getFilter()->isNull('age');

		$result = $posts->find($criteria)->first();

		$this->assertSame($data, (array)$result);

		$posts->updateOne($id, ['age' => 22]);
		$data['age'] = 22;

		$criteria->getFilter()->reset()->notNull('age');
		$user = $posts->find($criteria)->first();

		$this->assertSame($data, (array)$user);
	}
}

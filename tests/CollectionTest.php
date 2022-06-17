<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Semperton\Storage\MemoryStorage;
use Semperton\Search\Criteria;

final class CollectionTest extends TestCase
{
	public function testInsert(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$id = $users->insertOne([
			'name' => 'John'
		]);

		$this->assertEquals(1, $id);

		$name = $users->extract(1, 'name');
		$this->assertEquals('John', $name);
	}

	public function testInsertMany(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$ids = $users->insertMany([
			['name' => 'John'],
			['name' => 'Jane'],
			['name' => 'Luke']
		]);

		$this->assertSame([1, 2, 3], $ids);

		$name = $users->extract(2, 'name');
		$this->assertEquals('Jane', $name);
	}

	public function testGetValue(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$this->assertNull($users->extract(1, 'name'));

		$id = $users->insertOne(['name' => 'Kate']);

		$name = $users->extract($id, 'name');
		$this->assertEquals('Kate', $name);
	}

	public function testFind(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$user = $users->find(new Criteria(1))->first();

		$this->assertNull($user);

		$id = $users->insertOne(['name' => 'John']);

		$user = $users->find(new Criteria($id))->first();

		$this->assertIsObject($user);
		$this->assertEquals('John', $user->name);
	}

	public function testFindAll(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->create('users');

		$collection->insertMany([
			[
				'name' => 'John',
				'age' => 22,
			],
			[
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

		$id = $users->insertOne([
			'name' => 'John',
			'age' => 26
		]);

		$age = $users->extract($id, 'age');
		$this->assertEquals(26, $age);

		$status = $users->update(new Criteria($id), ['age' => 30]);
		$this->assertEquals(1, $status);

		$age = $users->extract($id, 'age');
		$this->assertEquals(30, $age);

		$status = $users->update(new Criteria($id), (object)[]);

		$this->assertEquals(1, $status);
	}

	public function testUpdateAll(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->create('data');

		$id = $collection->insertOne([
			'number' => 22
		]);

		$number = $collection->extract($id, 'number');

		$this->assertEquals(22, $number);

		$collection->update(new Criteria($id), [
			'number' => 33
		]);

		$number = $collection->extract($id, 'number');

		$this->assertEquals(33, $number);
	}

	public function testDelete(): void
	{
		$storage = new MemoryStorage();
		$collection = $storage->create('data');

		$id = $collection->insertOne([
			'number' => 22
		]);

		$number = $collection->extract($id, 'number');

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

		$id = $collection->insertOne([
			'number' => 22
		]);

		$number = $collection->extract($id, 'number');

		$this->assertEquals(22, $number);

		$collection->delete(new Criteria($id));

		$data = $collection->find(new Criteria($id))->first();

		$this->assertNull($data);
	}

	public function testIndex(): void
	{
		// UNIQUE constraint
		$this->expectException(Exception::class);

		$storage = new MemoryStorage();
		$collection = $storage->create('users');

		$res = $collection->createIndex('user.name', true);

		$this->assertTrue($res);

		$id = $collection->insertOne([
			'user' => [
				'name' => 'John',
				'age' => 22
			]
		]);

		$this->assertEquals(1, $id);

		$id2 = $collection->insertOne([
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
			[
				'title' => 'The Storage News',
				'number' => 22
			],
			[
				'title' => 'New Books Avail',
				'number' => 55
			]
		]);

		$comments->insertMany([
			[
				'posts_id' => 1,
				'title' => 'Good job',
				'content' => ''
			],
			[
				'posts_id' => 1,
				'title' => 'Nice read',
				'content' => ''
			]
		]);

		$criteria = new Criteria(1);
		$criteria = $criteria->withFields(['title']);

		$criteria = $criteria->withAssociation('comments', (new Criteria())->withFields(['title']));

		$data = $posts->find($criteria)->toArray();

		// var_dump($data);
	}

	public function testRaw(): void
	{
		$this->markTestSkipped();

		$storage = new MemoryStorage();
		$posts = $storage->create('posts');

		$id = $posts->insertOne([
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
			'rating' => 1.7,
			'new' => true,
		];

		$id = $posts->insertOne($data);
		$data['_id'] = $id;

		$this->assertIsFloat($posts->extract($id, 'rating'));
		$this->assertIsBool($posts->extract($id, 'new'));
		$this->assertIsInt($posts->extract($id, 'views'));

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

		$id = $posts->insertOne($data);
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

	public function testAggregations(): void
	{
		$storage = new MemoryStorage();
		$posts = $storage->create('posts');

		$posts->insertMany([
			[
				'title' => 'The Storage News',
				'number' => 22,
				'meta' => [
					'rating' => 3.2
				]
			],
			[
				'title' => 'New Books Avail',
				'number' => 55,
				'meta' => [
					'rating' => 4.7
				]
			]
		]);

		$criteria = new Criteria();
		$criteria = $criteria
			->withAvgAggregation('avg-rating', 'meta.rating')
			->withSumAggregation('sum-number', 'number');
		$result = $posts->find($criteria);

		// var_dump($result);

		$this->assertEquals(3.95, $result->getAggregation('avg-rating'));
		$this->assertEquals(77, $result->getAggregation('sum-number'));
	}

	public function testIndexes(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('user');
		$users->createIndex('meta.number', true);
		$users->createIndex('lastname');

		$indexes = $users->indexes();

		$expected = [
			[
				'name' => 'meta.number',
				'unique' => true
			],
			[
				'name' => 'lastname',
				'unique' => false
			]
		];

		$this->assertSame($expected, $indexes);

		$users->dropIndex('meta.number');
		$users->dropIndex('lastname');

		$this->assertSame([], $users->indexes());
	}

	public function testExtract(): void
	{
		$storage = new MemoryStorage();
		$misc = $storage->create('misc');

		$id = $misc->insertOne([
			'int' => 2,
			'float' => 3.2,
			'string' => 'Hello',
			'bool' => false,
			'null' => null,
			'object' => ['key' => 'value'],
			'array' => [1, 2, 3]
		]);

		$this->assertIsInt($misc->extract($id, 'int'));
		$this->assertIsFloat($misc->extract($id, 'float'));
		$this->assertIsString($misc->extract($id, 'string'));
		$this->assertIsBool($misc->extract($id, 'bool'));
		$this->assertNull($misc->extract($id, 'null'));
		$this->assertIsObject($misc->extract($id, 'object'));
		$this->assertIsArray($misc->extract($id, 'array'));

		$this->assertNull($misc->extract($id, 'undefined'));
	}

	public function testRename(): void
	{
		$storage = new MemoryStorage();
		$misc = $storage->create('misc');

		$this->assertTrue($storage->exists('misc'));

		$id = $misc->insertOne([
			'name' => 'John',
			'age' => 22
		]);

		$this->assertEquals(22, $misc->extract($id, 'age'));

		$misc->rename('users');

		$this->assertEquals('users', $misc->getName());

		$this->assertTrue($storage->exists('users'));
		$this->assertFalse($storage->exists('misc'));

		$users = $storage->get('users');

		$this->assertEquals('John', $users->extract($id, 'name'));
	}

	public function testReplace(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$data = [
			'name' => 'John',
			'age' => 22
		];

		$id = $users->insertOne($data);
		$data['_id'] = $id;

		$this->assertSame($data, (array)$users->findOne($id));

		$new = [
			'name' => 'Jane'
		];

		$users->replaceOne($id, $new);

		$new['_id'] = $id;

		$this->assertSame($new, (array)$users->findOne($id));
	}

	public function testDistinct(): void
	{
		$storage = new MemoryStorage();
		$users = $storage->create('users');

		$users->insertMany([
			['name' => 'John', 'age' => 22],
			['name' => 'Jane', 'age' => 18],
			['name' => 'Tom', 'age' => 31],
			['name' => 'Tamara', 'age' => 22],
			['name' => 'Jack', 'age' => 18]
		]);

		$criteria = (new Criteria())->withAscSort('age');
		$result = $users->distinct('age', $criteria);

		$this->assertSame([18, 22, 31], iterator_to_array($result));
	}

	public function testFields(): void
	{
		$storage = new MemoryStorage();
		$misc = $storage->create('misc');

		$id = $misc->insertOne([
			'firstname' => 'John',
			'lastname' => 'Doe',
			'age' => 22,
			'numbers' => [1, 2, 3],
			'rating' => 3.2,
			'id' => null
		]);

		$criteria = (new Criteria($id))->withFields(['_id' => false, 'age', 'rating']);
		$result = $misc->find($criteria)->first();

		$this->assertSame([
			'age' => 22,
			'rating' => 3.2,
		], (array)$result);
	}
}

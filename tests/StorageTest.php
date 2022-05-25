<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use Semperton\Storage\PersistentStorage;
use Semperton\Storage\MemoryStorage;
use Semperton\Search\Criteria;
use Semperton\Search\Filter;

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
		$id = $collection->insert((object)$obj);

		$criteria = new Criteria();
		$criteria->getFilter()->equal('_id', $id);

		$data = $collection->findAll($criteria);

		if (!is_array($data)) {
			$data = iterator_to_array($data);
		}

		$data = (array)$data[0];
		$obj['_id'] = 1;

		$this->assertEquals(1, $id);
		$this->assertSame($obj, $data);
	}

	public function testFileStorage(): void
	{
		$filepath = __DIR__ . '/storage.db';
		$storage = new PersistentStorage($filepath);

		$collection = $storage->create('misc');

		$obj = [
			'type' => 'collection',
			'name' => 'doc',
			'label' => 'Documents'
		];

		$id = $collection->insert((object)$obj);

		$data = (array)$collection->find($id);
		$obj['_id'] = 1;

		$this->assertSame($obj, $data);

		$deleted = unlink($filepath);

		$this->assertTrue($deleted);
		$this->assertFileDoesNotExist($filepath);
	}

	public function testExists(): void
	{
		$storage = new MemoryStorage();
		$this->assertFalse($storage->exists('misc'));
		$storage->create('misc');
		$this->assertTrue($storage->exists('misc'));
	}

	public function testRelations(): void
	{
		$this->markTestSkipped();

		$storage = new MemoryStorage();
		$posts = $storage->create('posts');
		$comments = $storage->create('comments');

		$postId = $posts->insert((object)[
			'title' => 'The Storage News',
			'number' => 22
		]);

		$comments->insertMany([
			(object)[
				'post_id' => $postId,
				'title' => 'Good job',
				'content' => ''
			],
			(object)[
				'post_id' => $postId,
				'title' => 'Nice read',
				'content' => ''
			]
		]);


		$post = $posts->find($postId);
		$cirteria = new Criteria();
		$cirteria->getFilter()->equal('post_id', $postId);
		$post->comments = $comments->findAll($cirteria)->toArray();
		$this->assertSame((array)$post, []);
	}
}

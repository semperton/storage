<?php

declare(strict_types=1);

namespace Semperton\Storage;

use PDO;
use Semperton\Database\Connection;
use Semperton\Query\QueryFactory;

final class MemoryStorage implements StorageInterface
{
	use StorageTrait;

	public function __construct(bool $persistent = false)
	{
		$this->connection = new Connection('sqlite::memory:', null, null, [
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_PERSISTENT => $persistent
		]);

		$this->queryFactory = new QueryFactory();
	}
}

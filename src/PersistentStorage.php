<?php

declare(strict_types=1);

namespace Semperton\Storage;

use PDO;
use Semperton\Database\Connection;
use Semperton\Query\QueryFactory;

final class PersistentStorage implements StorageInterface
{
	use StorageTrait;

	public function __construct(string $filepath)
	{
		$this->connection = new Connection('sqlite:' . $filepath, null, null, [
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_PERSISTENT => true // TODO: does this make sense?
		]);

		$this->queryFactory = new QueryFactory();
	}
}

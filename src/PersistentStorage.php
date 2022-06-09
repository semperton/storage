<?php

declare(strict_types=1);

namespace Semperton\Storage;

use PDO;

final class PersistentStorage extends Storage
{
	public function __construct(string $filepath)
	{
		$this->dsn = 'sqlite:' . $filepath;
		$this->options = [
			PDO::ATTR_PERSISTENT => true // TODO: does this make sense?
		];

		parent::__construct();
	}

	public function attach(string $filepath, string $alias): bool
	{
		$query = $this->queryFactory->raw('attach database :p1 as :p2');
		$query->bind('p1', $filepath)->bind('p2', $this->queryFactory->quoteIdentifier($alias));

		$sql = $query->compile($params);

		$result = $this->connection->execute($sql, $params);

		return $result;
	}

	public function detach(string $alias): bool
	{
		$query = $this->queryFactory->raw('detach database :p1');
		$query->bind('p1', $this->queryFactory->quoteIdentifier($alias));

		$sql = $query->compile($params);

		$result = $this->connection->execute($sql, $params);

		return $result;
	}
}

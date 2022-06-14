<?php

declare(strict_types=1);

namespace Semperton\Storage;

final class PersistentStorage extends Storage
{
	public function __construct(string $filepath)
	{
		$this->filepath = $filepath;
		parent::__construct();
	}

	public function attach(string $filepath, string $alias): bool
	{
		$factory = $this->queryFactory;

		$query = $factory->raw('attach database :p1 as :p2');
		$query->bind('p1', $filepath)->bind('p2', $factory->quoteIdentifier($alias));

		$sql = $query->compile($params);

		$result = $this->connection->execute($sql, $params);

		return $result;
	}

	public function detach(string $alias): bool
	{
		$factory = $this->queryFactory;
		
		$query = $factory->raw('detach database :p1');
		$query->bind('p1', $factory->quoteIdentifier($alias));

		$sql = $query->compile($params);

		$result = $this->connection->execute($sql, $params);

		return $result;
	}
}

<?php

declare(strict_types=1);

namespace Semperton\Storage;

use Semperton\Database\ConnectionInterface;
use Semperton\Query\QueryFactory;

trait StorageTrait
{
	/** @var ConnectionInterface */
	protected $connection;

	/** @var QueryFactory */
	protected $queryFactory;

	// TODO: capability function
	// check pragma compile_options for ENABLE_JSON1 and ENABLE_UPDATE_DELETE_LIMIT

	public function get(string $name): CollectionInterface
	{
		return new Collection($name, $this->connection, $this->queryFactory);
	}

	public function exists(string $name): bool
	{
		return $this->get($name)->valid();
	}

	public function create(string $name): CollectionInterface
	{
		$table = $this->queryFactory->quoteIdentifier($name);

		$sql = "create table if not exists $table (id integer not null primary key, data text not null)";
		$this->connection->execute($sql);

		return $this->get($name);
	}

	public function delete(string $name): bool
	{
		$query = $this->queryFactory->drop($name)->exists();
		$sql = $query->compile($params);

		return $this->connection->execute($sql, $params);
	}
}

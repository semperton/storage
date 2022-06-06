<?php

declare(strict_types=1);

namespace Semperton\Storage;

use Semperton\Database\Connection;
use Semperton\Database\ConnectionInterface;
use Semperton\Query\QueryFactory;

abstract class Storage implements StorageInterface
{
	/** @var string */
	protected $dsn = '';

	/** @var array */
	protected $options = [];

	/** @var ConnectionInterface */
	protected $connection;

	/** @var QueryFactory */
	protected $queryFactory;

	public function __construct()
	{
		$this->connection = new Connection($this->dsn, null, null, $this->options);
		$this->queryFactory = new QueryFactory();
	}

	// TODO: capability function
	// check pragma compile_options for ENABLE_JSON1 and ENABLE_UPDATE_DELETE_LIMIT

	public function get(string $collection): CollectionInterface
	{
		return new Collection($collection, $this, $this->connection, $this->queryFactory);
	}

	public function exists(string $collection): bool
	{
		$query = $this->queryFactory->select('sqlite_master');
		$query->fields([$query->raw('1')])->where('type', '=', 'table')->where('name', '=', $collection);

		$sql = $query->compile($params);

		/** @psalm-suppress PossiblyNullArgument */
		$result = (int)$this->connection->fetchValue($sql, $params);

		return (bool)$result;
	}

	public function create(string $collection): CollectionInterface
	{
		$table = $this->queryFactory->quoteIdentifier($collection);
		$sql = 'create table if not exists ' . $table . ' (id integer not null primary key, data text not null)';

		$this->connection->execute($sql);

		return $this->get($collection);
	}

	public function delete(string $collection): bool
	{
		$query = $this->queryFactory->drop($collection)->exists();
		$sql = $query->compile($params);

		/** @psalm-suppress PossiblyNullArgument */
		return $this->connection->execute($sql, $params);
	}
}

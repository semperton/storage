<?php

declare(strict_types=1);

namespace Semperton\Storage;

use Semperton\Database\ConnectionInterface;
use Semperton\Database\SQLiteConnection;
use Semperton\Query\QueryFactory;

abstract class Storage implements StorageInterface
{
	/** @var string */
	protected $filepath = ':memory:';

	/** @var ConnectionInterface */
	protected $connection;

	/** @var QueryFactory */
	protected $queryFactory;

	public function __construct()
	{
		$this->connection = new SQLiteConnection($this->filepath);
		$this->queryFactory = new QueryFactory(true);
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

		$result = (int)$this->connection->fetchValue($sql, $params);

		return (bool)$result;
	}

	public function create(string $collection): CollectionInterface
	{
		$table = $this->queryFactory->quoteIdentifier($collection);
		$sql = 'create table if not exists ' . $table . ' (id integer primary key, data text not null)';

		$this->connection->execute($sql);

		return $this->get($collection);
	}

	public function delete(string $collection): bool
	{
		$query = $this->queryFactory->drop($collection)->exists();
		$sql = $query->compile($params);

		return $this->connection->execute($sql, $params);
	}

	public function collections(): array
	{
		$query = $this->queryFactory->select('sqlite_master');
		$query->fields(['name'])->where('type', '=', 'table');

		$sql = $query->compile($params);

		$result = $this->connection->fetchResult($sql, $params)->toArray();

		/** @var string[] */
		return array_column($result, 'name');
	}

	public function indexes(?string $collection = null): array
	{
		$query = $this->queryFactory->select('sqlite_master');
		$query->fields([
			'name',
			'collection' => 'tbl_name',
			'unique' => $query->raw("substr(sql, 1, 13) = 'CREATE UNIQUE'")
		])->where('type', '=', 'index');

		if ($collection !== null) {
			$query->where('tbl_name', '=', $collection);
		}

		$sql = $query->compile($params);

		$result = $this->connection->fetchResult($sql, $params)->toArray();

		foreach ($result as &$entry) {
			$entry['unique'] = (bool)$entry['unique'];
		}

		/** @var array{name: string, collection: string, unique: bool}[] */
		return $result;
	}
}

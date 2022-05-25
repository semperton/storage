<?php

declare(strict_types=1);

namespace Semperton\Storage;

use Generator;
use InvalidArgumentException;
use Iterator;
use Semperton\Database\ConnectionInterface;
use Semperton\Query\QueryFactory;
use Semperton\Search\Condition;
use Semperton\Search\Criteria;
use Semperton\Query\Expression\Filter as QueryFilter;
use Semperton\Search\Filter as SearchFilter;
use Semperton\Search\Result;

final class Collection implements CollectionInterface
{
	/** @var string */
	protected $name;

	/** @var ConnectionInterface */
	protected $connection;

	/** @var QueryFactory */
	protected $queryFactory;

	public function __construct(string $name, ConnectionInterface $connection, QueryFactory $queryFactory)
	{
		$this->name = $name;
		$this->connection = $connection;
		$this->queryFactory = $queryFactory;
	}

	protected function encode($data): string
	{
		return json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
	}

	protected function decode(string $data)
	{
		return json_decode($data, false, 512, JSON_THROW_ON_ERROR);
	}

	public function valid(): bool
	{
		$query = $this->queryFactory->select('sqlite_master');
		$query->fields([$query->raw('1')])->where('type', '=', 'table')->where('name', '=', $this->name);

		$sql = $query->compile($params);
		$result = $this->connection->fetchValue($sql, $params);

		return $result ? true : false;
	}

	public function addIndex(string $field, bool $unique = false): bool
	{
		$indexName = $this->queryFactory->escapeString($field);

		$tableName = $this->queryFactory->quoteIdentifier($this->name);

		$path = '$.' . $this->queryFactory->escapeString($field);

		$expr = "json_extract(data, '$path')";
		// $sql = 'create' . ($unique ? ' unique' : '') . " index if not exists $indexName on $tableName($expr) where $expr";
		$sql = 'create' . ($unique ? ' unique' : '') . " index if not exists '$indexName' on $tableName($expr)";

		$result = $this->connection->execute($sql);

		return $result;
	}

	public function removeIndex(string $field): bool
	{
		$indexName = $this->queryFactory->escapeString($field);
		$result = $this->connection->execute("drop index if exists '$indexName'");
		return $result;
	}

	public function insert(object $data): ?int
	{
		$query = $this->queryFactory->insert($this->name);
		$value = $query->func('json', $this->encode($data));
		$query->values(['data' => $value]);

		$sql = $query->compile($params);

		$result = $this->connection->execute($sql, $params);

		return $result ? $this->connection->lastInsertId() : null;
	}

	//TODO: use transactions
	public function insertMany(array $data): array
	{
		$ids = [];

		// $this->connection->execute('begin transaction');

		foreach ($data as $obj) {
			$ids[] = $this->insert($obj);
		}

		// $this->connection->execute('commit transaction');

		return $ids;
	}

	public function update(int $id, object $data): bool
	{
		$searchFilter = new SearchFilter();
		$searchFilter->equal('_id', $id);

		return $this->updateAll($searchFilter, $data, 1) > 0;
	}

	public function updateAll(SearchFilter $searchFilter, object $data, int $limit = 0): int
	{
		$queryFilter = new QueryFilter($this->queryFactory);
		$this->addQueryFilter($queryFilter, $searchFilter);

		if (!$queryFilter->valid()) {
			return 0;
		}

		$query = $this->queryFactory->update($this->name);
		$value = $query->func('json_patch', $query->ident('data'), $query->func('json', $this->encode($data)));

		// TODO: implement limit
		// SQLite support with ENABLE_UPDATE_DELETE_LIMIT
		$query->set('data', $value)->where($queryFilter); // ->orderAsc('id')->limit($limit);

		$sql = $query->compile($params);

		$this->connection->execute($sql, $params);

		return $this->connection->affectedRows();
	}

	public function delete(int $id): bool
	{
		$searchFilter = new SearchFilter();
		$searchFilter->equal('_id', $id);

		return $this->deleteAll($searchFilter, 1) > 0;
	}

	public function deleteAll(SearchFilter $searchFilter, int $limit = 0): int
	{
		$queryFilter = new QueryFilter($this->queryFactory);
		$this->addQueryFilter($queryFilter, $searchFilter);

		if (!$queryFilter->valid()) {
			return 0;
		}

		// TODO: implement limit
		// SQLite support with ENABLE_UPDATE_DELETE_LIMIT
		$query = $this->queryFactory->delete($this->name)->where($queryFilter); // ->orderAsc('id')->limit($limit);

		$sql = $query->compile($params);

		$this->connection->execute($sql, $params);

		return $this->connection->affectedRows();
	}

	protected function prepareCompareValue($value): string
	{
		if (is_string($value)) {
			return "'" . $this->queryFactory->escapeString($value) . "'";
		}

		if (is_bool($value)) {
			return (string)(int)$value;
		}

		if (is_array($value)) {
			array_map([$this, 'prepareCompareValue'], $value);
			return '(' . implode(', ', $value) . ')';
		}

		return (string)$value;
	}

	protected function addQueryFilter(QueryFilter $queryFilter, SearchFilter $searchFilter): void
	{
		foreach ($searchFilter as $connection => $entry) {

			$connect = $connection === $searchFilter::CONNECTION_AND ? 'and' : 'or';

			if ($entry instanceof SearchFilter) {
				$subFilter = new QueryFilter($this->queryFactory);
				$this->addQueryFilter($subFilter, $entry);
				// check if filter is valid
				if ($subFilter->valid()) {
					$queryFilter->$connect($subFilter);
				}
			} else if ($entry instanceof Condition) {

				$field = $entry->getField();

				// check for system field
				// TODO: check if field is valid
				if ($field[0] === '_') {

					$field = substr($field, 1);
					$queryFilter->$connect($field, $entry->getOperator(), $entry->getValue());
				} else {

					$expr = $this->queryFactory->func('json_extract', $this->queryFactory->ident('data'), "\$.$field");

					// FIXME: prepared statement does not use correct data type, when comparing json_extract values
					$value = $entry->getValue();
					$rawValue = $this->prepareCompareValue($value);

					$raw = $this->queryFactory->raw($rawValue);

					$queryFilter->$connect($expr, $entry->getOperator(), $raw);
				}
			}
		}
	}

	public function find(int $id): ?object
	{
		$query = $this->queryFactory->select($this->name);
		$field = $query->func('json_set', $query->ident('data'), '$._id', $query->ident('id'));
		$query->fields([$field])->where('id', '=', $id)->limit(1);

		$sql = $query->compile($params);

		$result = $this->connection->fetchValue($sql, $params);

		return $result ? $this->decode($result) : null;
	}

	public function findAll(Criteria $criteria): Result
	{
		$query = $this->queryFactory->select($this->name);

		// fields
		$fields = $criteria->getFields();

		if ($fields) {

			$args = ['{}'];
			foreach ($fields as $field) {
				$args[] = $path = '$.' . $field;
				$args[] = $query->func('json_extract', $query->ident('data'), $path);
			}

			// add system fields
			$args[] = '$._id';
			$args[] = $query->ident('id');

			$field = $query->func('json_set', ...$args);
		} else {

			$field = $query->func('json_set', $query->ident('data'), '$._id', $query->ident('id'));
		}

		$query->fields(['json' => $field]);

		// limit, offset
		$query->limit($criteria->getLimit())->offset($criteria->getOffset());

		// filters
		$searchFilter = $criteria->getFilter();
		$queryFilter = new QueryFilter($this->queryFactory);
		$this->addQueryFilter($queryFilter, $searchFilter);

		// check if filter is valid
		if ($queryFilter->valid()) {
			$query->where($queryFilter);
		}

		// sorting
		$sorting = $criteria->getSorting();
		foreach ($sorting as $field => $order) {

			// check for system fields
			// TODO: check if field is valid
			if ($field[0] === '_') {

				$expr = substr($field, 1);
			} else {
				$expr = $query->func('json_extract', $query->ident('data'), "\$.$field");
			}

			if ($order === $criteria::SORT_DESC) {
				$query->orderDesc($expr);
			} else {
				$query->orderAsc($expr);
			}
		}

		$sql = $query->compile($params);
		$result = $this->connection->fetchAll($sql, $params);

		$rows = $this->decodeRows($result);

		return new Result($criteria, $rows);
	}

	protected function decodeRows(?Iterator $result): Generator
	{
		if ($result) {
			foreach ($result as $row) {
				yield $this->decode($row['json']);
			}
		}
	}

	public function getValue(int $id, string $field)
	{
		$path = '$.' . $field;

		$query = $this->queryFactory->select($this->name);

		$value = $query->func('json_extract', $query->ident('data'), $path);
		$type = $query->func('json_type', $query->ident('data'), $path);

		$query->fields(['value' => $value, 'type' => $type])->where('id', '=', $id)->limit(1);

		$sql = $query->compile($params);
		$result = $this->connection->fetchRow($sql, $params);

		if ($result !== null) {
			$result = $this->convertJsonValue($result['value'], $result['type']);
		}

		return $result;
	}

	/**
	 * Converts a SQLite JSON1 extension type to the appropriate PHP value
	 * https://www.sqlite.org/json1.html#the_json_type_function
	 * @return mixed
	 */
	protected function convertJsonValue($value, string $type)
	{
		switch ($type) {
			case 'null':
				$value = null;
				break;
			case 'true':
				$value = true;
				break;
			case 'false':
				$value = false;
				break;
			case 'integer':
				$value = (int)$value;
				break;
			case 'real':
				$value = (float)$value;
				break;
			case 'text':
				$value = (string)$value;
				break;
			case 'array':
			case 'object':
				$value = $this->decode($value);
				break;
			default:
				throw new InvalidArgumentException("< $type > is not a valid JSON1 type");
		}

		return $value;
	}
}

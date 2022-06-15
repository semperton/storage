<?php

declare(strict_types=1);

namespace Semperton\Storage;

use Generator;
use InvalidArgumentException;
use Semperton\Database\ConnectionInterface;
use Semperton\Query\QueryFactory;
use Semperton\Search\Criteria;
use Semperton\Query\Expression\Filter as QueryFilter;
use Semperton\Query\ExpressionInterface;
use Semperton\Query\Type\SelectQuery;
use Semperton\Search\Filter as SearchFilter;

final class Collection implements CollectionInterface
{
	use TransformTrait;

	/** @var string */
	protected $name;

	/** @var StorageInterface */
	protected $storage;

	/** @var ConnectionInterface */
	protected $connection;

	/** @var QueryFactory */
	protected $queryFactory;

	public function __construct(
		string $name,
		StorageInterface $storage,
		ConnectionInterface $connection,
		QueryFactory $queryFactory
	) {
		$this->name = $name;
		$this->storage = $storage;
		$this->connection = $connection;
		$this->queryFactory = $queryFactory;
	}

	public function getName(): string
	{
		return $this->name;
	}

	public function createIndex(string $field, bool $unique = false): bool
	{
		$indexName = $this->queryFactory->escapeString($field);
		$tableName = $this->queryFactory->quoteIdentifier($this->name);

		$path = '$.' . $indexName;

		$expr = "json_extract(data, '$path')";
		// $sql = 'create' . ($unique ? ' unique' : '') . " index if not exists $indexName on $tableName($expr) where $expr";
		$sql = 'create' . ($unique ? ' unique' : '') . " index if not exists '$indexName' on $tableName($expr)";

		$result = $this->connection->execute($sql);

		return $result;
	}

	public function dropIndex(string $field): bool
	{
		$indexName = $this->queryFactory->escapeString($field);
		$result = $this->connection->execute("drop index if exists '$indexName'");

		return $result;
	}

	public function indexes(): array
	{
		$query = $this->queryFactory->select('sqlite_master');
		$unique = $this->queryFactory->quoteIdentifier('unique');
		$query->fields([
			'name',
			$unique => $query->raw("substr(sql, 1, 13) = 'CREATE UNIQUE'")
		])->where('type', '=', 'index')->where('tbl_name', '=', $this->name);

		$sql = $query->compile($params);

		$result = $this->connection->fetchResult($sql, $params)->toArray();

		foreach ($result as &$entry) {
			$entry['unique'] = (bool)$entry['unique'];
		}

		/** @var array{name: string, unique: bool}[] */
		return $result;
	}

	public function insertOne($data): int
	{
		$query = $this->queryFactory->insert($this->name);
		$value = $query->func('json', $this->encode($data));
		$query->values(['data' => $value]);

		$sql = $query->compile($params);

		$result = $this->connection->execute($sql, $params);

		return $result ? $this->connection->lastInsertId() : 0;
	}

	public function insertMany(iterable $data): array
	{
		$query = $this->queryFactory->insert($this->name);
		$query->values(['data' => $query->raw('json(:data)')]);

		$sql = $query->compile();
		$ids = [];

		$this->connection->beginTransaction();

		foreach ($data as $obj) {

			$json = $this->encode($obj);
			$result = $this->connection->execute($sql, ['data' => $json]);

			$ids[] = $result ? $this->connection->lastInsertId() : 0;
		}

		$this->connection->commit();

		return $ids;
	}

	public function update(Criteria $criteria, $data): int
	{
		$queryFilter = $this->buildQueryFilter($criteria);

		if (!$queryFilter->valid()) {
			return 0;
		}

		$query = $this->queryFactory->update($this->name);
		$value = $query->func('json_patch', $query->ident('data'), $this->encode($data));

		// SQLite support with ENABLE_UPDATE_DELETE_LIMIT
		// $limit = $criteria->getLimit();
		$query->set('data', $value)->where($queryFilter); // ->orderAsc('id')->limit($limit);

		$sql = $query->compile($params);

		$this->connection->execute($sql, $params);

		return $this->connection->affectedRows();
	}

	public function updateOne(int $id, $data): bool
	{
		$result = $this->update(new Criteria($id), $data);

		return $result > 0;
	}

	public function replaceOne(int $id, $data): bool
	{
		$query = $this->queryFactory->update($this->name);
		$value = $query->func('json', $this->encode($data));

		$query->set('data', $value)->where('id', '=', $id);

		$sql = $query->compile($params);

		return $this->connection->execute($sql, $params);
	}

	public function delete(Criteria $criteria): int
	{
		$queryFilter = $this->buildQueryFilter($criteria);

		if (!$queryFilter->valid()) {
			return 0;
		}

		// SQLite support with ENABLE_UPDATE_DELETE_LIMIT
		// $limit = $criteria->getLimit();
		$query = $this->queryFactory->delete($this->name)->where($queryFilter); // ->orderAsc('id')->limit($limit);

		$sql = $query->compile($params);

		$this->connection->execute($sql, $params);

		return $this->connection->affectedRows();
	}

	public function deleteOne(int $id): bool
	{
		$result = $this->delete(new Criteria($id));

		return $result > 0;
	}

	public function find(Criteria $criteria): ObjectResult
	{
		$result = $this->search($criteria);
		$aggregations = $this->fetchAggregations($criteria);

		return new ObjectResult($criteria, $result, $aggregations);
	}

	public function findRaw(Criteria $criteria): StringResult
	{
		$result = $this->search($criteria);
		$aggregations = $this->fetchAggregations($criteria);

		return new StringResult($criteria, $result, $aggregations);
	}

	protected function search(Criteria $criteria): Generator
	{
		$fields = $this->buildSearchFields($criteria);

		$query = $this->buildSearchQuery($criteria)->from($this->name)->fields([$fields]);

		$sql = $query->compile($params);

		return $this->connection->fetchColumn($sql, $params);
	}

	/**
	 * @return array<string, mixed>
	 */
	protected function fetchAggregations(Criteria $criteria): array
	{
		if (!$criteria->hasAggregations()) {
			return [];
		}

		$fields = $this->buildAggregationFields($criteria);

		$query = $this->buildSearchQuery($criteria)->from($this->name)->fields($fields);

		$sql = $query->compile($params);

		$result = $this->connection->fetchRow($sql, $params);

		return $result ?? [];
	}

	/**
	 * @return array<string, ExpressionInterface>
	 */
	protected function buildAggregationFields(Criteria $criteria): array
	{
		$factory = $this->queryFactory;

		$aggregations = $criteria->getAggregations();

		$fields = [];

		foreach ($aggregations as $name => $agg) {

			$expr = $this->buildFieldSelector($agg->getField());
			$name = $factory->quoteIdentifier($name);

			$fields[$name] = $factory->func($agg->getType(), $expr);
		}

		return $fields;
	}

	public function findOne(int $id): ?object
	{
		/** @var null|object */
		return $this->find(new Criteria($id))->first();
	}

	public function findOneRaw(int $id): ?string
	{
		/** @var null|string */
		return $this->findRaw(new Criteria($id))->first();
	}

	public function extract(int $id, string $field)
	{
		$query = $this->queryFactory->select($this->name);

		$path = '$.' . $field;
		$col = $query->ident('data');

		$value = $query->func('json_extract', $col, $path);
		$type = $query->func('json_type', $col, $path);

		$query->fields(['value' => $value, 'type' => $type])->where('id', '=', $id)->limit(1);

		$sql = $query->compile($params);

		$result = $this->connection->fetchRow($sql, $params);

		if ($result !== null && isset($result['value'])) {

			return $this->convertJsonValue($result['value'], (string)$result['type']);
		}

		return null;
	}

	public function distinct(string $field, ?Criteria $criteria = null): iterable
	{
		$query = $criteria ? $this->buildSearchQuery($criteria) : new SelectQuery($this->queryFactory);
		$query->from($this->name)->distinct();

		$path = '$.' . $field;
		$value = $query->func('json_extract', $query->ident('data'), $path);

		$query->fields([$value]);

		$sql = $query->compile($params);

		return $this->connection->fetchColumn($sql, $params);
	}

	public function rename(string $name): bool
	{
		$factory = $this->queryFactory;

		if (!$this->storage->exists($name)) {

			$current = $factory->quoteIdentifier($this->name);
			$new = $factory->quoteIdentifier($name);

			$sql = 'alter table ' . $current . ' rename to ' . $new;

			if ($this->connection->execute($sql)) {

				$this->name = $name;
				return true;
			}
		}

		return false;
	}

	protected function buildSearchFields(Criteria $criteria): ExpressionInterface
	{
		$factory = $this->queryFactory;

		$fields = $criteria->getFields();
		$associations = $criteria->getAssociations();

		$dataIdent = $factory->ident('data');

		$args = !!$fields ? ['{}'] : [$dataIdent];

		foreach ($fields as $field) {

			$path = '$.' . $field;

			$args[] = $path;
			$args[] = $factory->func('json_extract', $dataIdent, $path);
		}

		// associations
		// we auto join on {entity}_id = {entity}.id
		if ($associations) {

			$conn = $factory->func('json_extract', $dataIdent, '$.' . $this->name . '_id');
			$idIdent = $factory->ident($this->name . '.id');

			foreach ($associations as $coll => $crit) {

				$subFields = $this->buildSearchFields($crit);
				$subSelect = $this->buildSearchQuery($crit)
					->from($coll)
					->fields([$factory->func('json_group_array', $subFields)])
					->where($conn, '=', $idIdent);

				$args[] = '$.$' . $coll;
				$args[] = $subSelect;
			}
		}

		// system fields
		$args[] = '$._id';
		$args[] = $factory->ident('id');

		$expr = $factory->func('json_set', ...$args);

		return $expr;
	}

	protected function buildQueryFilter(Criteria $criteria): QueryFilter
	{
		$queryFilter = new QueryFilter($this->queryFactory);

		// $ids
		$ids = $criteria->getIds();

		if ($ids) {
			$queryFilter->and('id', 'in', $ids);
		} else if ($criteria->hasFilter()) {
			$this->addSearchFilter($queryFilter, $criteria->getFilter());
		}

		return $queryFilter;
	}

	protected function buildSearchQuery(Criteria $criteria): SelectQuery
	{
		$factory = $this->queryFactory;
		$query = new SelectQuery($factory);

		// TODO: source this out
		$queryFilter = $this->buildQueryFilter($criteria);

		// check if filter is valid
		if ($queryFilter->valid()) {
			$query->where($queryFilter);
		}

		// limit, offset
		$limit = $criteria->getLimit();
		$query->limit($limit > 0 ? $limit : count($criteria->getIds()));
		$query->offset($criteria->getOffset());

		// sorting
		$sorting = $criteria->getSorting();
		foreach ($sorting as $field => $order) {

			$expr = $this->buildFieldSelector($field);

			if ($order === $criteria::SORT_DESC) {
				$query->orderDesc($expr);
			} else {
				$query->orderAsc($expr);
			}
		}

		return $query;
	}

	protected function buildFieldSelector(string $field): ExpressionInterface
	{
		if ($field === '') {
			throw new InvalidArgumentException('Field selector cannot be empty');
		}

		$factory = $this->queryFactory;

		// check for system field
		// TODO: check if field is valid
		if ($field[0] === '_') {
			$field = substr($field, 1);
			return $factory->ident($field);
		}

		return $factory->func('json_extract', $factory->ident('data'), "\$.$field");
	}

	protected function addSearchFilter(QueryFilter $queryFilter, SearchFilter $searchFilter): void
	{
		$factory = $this->queryFactory;

		foreach ($searchFilter as $connection => $entry) {

			$connect = $connection === $searchFilter::CONNECTION_AND ? 'and' : 'or';

			if ($entry instanceof SearchFilter) {

				$subFilter = new QueryFilter($factory);

				$this->addSearchFilter($subFilter, $entry);

				// check if filter is valid
				if ($subFilter->valid()) {
					$queryFilter->$connect($subFilter);
				}

				continue;
			}

			// condition
			$field = $this->buildFieldSelector($entry->getField());

			$queryFilter->$connect($field, $entry->getOperator(), $entry->getValue());
		}
	}
}

<?php

declare(strict_types=1);

namespace Semperton\Storage;

use Semperton\Database\ConnectionInterface;
use Semperton\Database\ResultSetInterface;
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

	public function valid(): bool
	{
		return $this->storage->exists($this->name);
	}

	public function addIndex(string $field, bool $unique = false): bool
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

	public function removeIndex(string $field): bool
	{
		$indexName = $this->queryFactory->escapeString($field);
		$result = $this->connection->execute("drop index if exists '$indexName'");
		return $result;
	}

	public function indexes(): array
	{
		return $this->storage->indexes($this->name);
	}

	public function insert($data): int
	{
		$query = $this->queryFactory->insert($this->name);
		$value = $query->func('json', $this->encode($data));
		$query->values(['data' => $value]);

		$sql = $query->compile($params);

		/** @psalm-suppress PossiblyNullArgument */
		$result = $this->connection->execute($sql, $params);

		return $result ? $this->connection->lastInsertId() : 0;
	}

	public function insertMany(array $data): array
	{
		$ids = [];

		foreach ($data as $obj) {
			$ids[] = $this->insert($obj);
		}

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

		/** @psalm-suppress PossiblyNullArgument */
		$this->connection->execute($sql, $params);

		return $this->connection->affectedRows();
	}

	public function updateOne(int $id, $data): bool
	{
		$result = $this->update(new Criteria($id), $data);

		return $result > 0;
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

		/** @psalm-suppress PossiblyNullArgument */
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

		return new ObjectResult($criteria, $result);
	}

	public function findRaw(Criteria $criteria): StringResult
	{
		$result = $this->search($criteria);

		return new StringResult($criteria, $result);
	}

	protected function search(Criteria $criteria): ResultSetInterface
	{
		$fields = $this->buildSearchFields($criteria);

		$query = $this->buildSearchQuery($criteria)->from($this->name)->fields(['json' => $fields]);

		$sql = $query->compile($params);

		/** @psalm-suppress PossiblyNullArgument */
		return $this->connection->fetchResult($sql, $params);
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

	public function getValue(int $id, string $field)
	{
		$query = $this->queryFactory->select($this->name);

		$path = '$.' . $field;
		$col = $query->ident('data');

		$value = $query->func('json_extract', $col, $path);
		$type = $query->func('json_type', $col, $path);

		$query->fields(['value' => $value, 'type' => $type])->where('id', '=', $id)->limit(1);

		$sql = $query->compile($params);

		/** @psalm-suppress PossiblyNullArgument */
		$result = $this->connection->fetchRow($sql, $params);

		if ($result !== null) {
			$result = $this->convertJsonValue((string)$result['value'], (string)$result['type']);
		}

		return $result;
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
		} else {
			$this->addSearchFilter($queryFilter, $criteria->getFilter());
		}

		return $queryFilter;
	}

	protected function buildSearchQuery(Criteria $criteria): SelectQuery
	{
		$factory = $this->queryFactory;
		$query = new SelectQuery($factory);

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

		return $query;
	}

	/**
	 * @param null|scalar|array $value
	 */
	protected function prepareCompareValue($value): string
	{
		if (is_string($value)) {
			$escapeStr = $this->queryFactory->getEscapeString();
			return $escapeStr . $this->queryFactory->escapeString($value) . $escapeStr;
		}

		if (is_bool($value)) {
			return (string)(int)$value;
		}

		if (is_array($value)) {
			array_map([$this, 'prepareCompareValue'], $value);
			/** @var string[] $value */
			return '(' . implode(', ', $value) . ')';
		}

		return (string)$value;
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

			$field = $entry->getField();
			$value = $entry->getValue();

			// check for system field
			// TODO: check if field is valid
			if ($field !== '' && $field[0] === '_') {
				$field = substr($field, 1);
			} else {

				$field = $factory->func('json_extract', $factory->ident('data'), "\$.$field");
			}

			// prepared statement does not use correct data type
			// when comparing json_extract values - so we do it manually
			$rawValue = $this->prepareCompareValue($value);
			$value = $factory->raw($rawValue);

			$queryFilter->$connect($field, $entry->getOperator(), $value);
		}
	}
}

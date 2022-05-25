<?php

declare(strict_types=1);

namespace Semperton\Storage;

use JsonSerializable;
use Semperton\Search\Criteria;
use Semperton\Search\Filter;
use Semperton\Search\Result;
use stdClass;

interface CollectionInterface
{
	public function valid(): bool;
	/**
	 * @param stdClass|JsonSerializable $data
	 */
	public function insert(object $data): ?int;
	/**
	 * @param (stdClass|JsonSerializable)[] $data
	 * @return int[]
	 */
	public function insertMany(array $data): array;
	public function find(int $id): ?object;
	public function findAll(Criteria $criteria): Result;
	/**
	 * @return mixed
	 */
	public function getValue(int $id, string $field);
	/**
	 * @param stdClass|JsonSerializable $data
	 */
	public function update(int $id, object $data): bool;
	/**
	 * @param stdClass|JsonSerializable $data
	 */
	public function updateAll(Filter $filter, object $data, int $limit = 0): int;
	public function delete(int $id): bool;
	public function deleteAll(Filter $filter, int $limit = 0): int;
	public function addIndex(string $field, bool $unique): bool;
	public function removeIndex(string $field): bool;
}

<?php

declare(strict_types=1);

namespace Semperton\Storage;

use JsonSerializable;
use Semperton\Search\Criteria;

interface CollectionInterface
{
	public function valid(): bool;

	/**
	 * @param array|object|JsonSerializable $data
	 */
	public function insert($data): int;

	/**
	 * @param (array|object|JsonSerializable)[] $data
	 * @return int[]
	 */
	public function insertMany(array $data): array;

	public function find(Criteria $criteria): ObjectResult;

	public function findRaw(Criteria $criteria): StringResult;

	public function findOne(int $id): ?object;

	public function findRawOne(int $id): ?string;

	/**
	 * @return null|mixed
	 */
	public function getValue(int $id, string $field);

	/**
	 * @param array|object|JsonSerializable $data
	 */
	public function update(Criteria $criteria, $data): int;

	/**
	 * @param array|object|JsonSerializable $data
	 */
	public function updateOne(int $id, $data): bool;

	public function delete(Criteria $criteria): int;

	public function deleteOne(int $id): bool;

	public function addIndex(string $field, bool $unique): bool;

	public function removeIndex(string $field): bool;
}

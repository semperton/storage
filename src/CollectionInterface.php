<?php

declare(strict_types=1);

namespace Semperton\Storage;

use JsonSerializable;
use Semperton\Search\Criteria;
use stdClass;

interface CollectionInterface
{
	public function getName(): string;

	/**
	 * @param array<string, mixed>|stdClass|JsonSerializable $data
	 */
	public function insertOne($data): int;

	/**
	 * @param iterable<array-key, array<string, mixed>|stdClass|JsonSerializable> $data
	 * @return int[]
	 */
	public function insertMany(iterable $data): array;

	public function find(Criteria $criteria): ObjectResult;

	public function findRaw(Criteria $criteria): StringResult;

	public function findOne(int $id): ?object;

	public function findOneRaw(int $id): ?string;

	/**
	 * @return null|scalar|array|stdClass
	 */
	public function extract(int $id, string $field);

	/**
	 * @return iterable<array-key, mixed>
	 */
	public function distinct(string $field, ?Criteria $criteria = null): iterable;

	/**
	 * @param array<string, mixed>|stdClass|JsonSerializable $data
	 */
	public function update(Criteria $criteria, $data): int;

	/**
	 * @param array<string, mixed>|stdClass|JsonSerializable $data
	 */
	public function updateOne(int $id, $data): bool;

	/**
	 * @param array<string, mixed>|stdClass|JsonSerializable $data
	 */
	public function replaceOne(int $id, $data): bool;

	public function delete(Criteria $criteria): int;

	public function deleteOne(int $id): bool;

	public function createIndex(string $field, bool $unique = false): bool;

	public function dropIndex(string $field): bool;

	/**
	 * @return array{name: string, unique: bool}[]
	 */
	public function indexes(): array;

	public function rename(string $name): bool;
}

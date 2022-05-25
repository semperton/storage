<?php

declare(strict_types=1);

namespace Semperton\Storage;

interface StorageInterface
{
	public function create(string $collection): CollectionInterface;
	public function exists(string $collection): bool;
	public function delete(string $collection): bool;
	public function get(string $collection): CollectionInterface;
}

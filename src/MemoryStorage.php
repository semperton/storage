<?php

declare(strict_types=1);

namespace Semperton\Storage;

use PDO;

final class MemoryStorage extends Storage
{
	public function __construct(bool $persistent = false)
	{
		$this->dsn = 'sqlite::memory:';
		$this->options = [
			PDO::ATTR_PERSISTENT => $persistent
		];

		parent::__construct();
	}
}

<?php

declare(strict_types=1);

namespace Semperton\Storage;

final class MemoryStorage extends Storage
{
	public function __construct()
	{
		$this->filepath = ':memory:';
		parent::__construct();
	}
}

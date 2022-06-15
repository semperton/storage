<?php

declare(strict_types=1);

namespace Semperton\Storage;

use Semperton\Search\Result;

final class StringResult extends Result
{
	public function current()
	{
		/** @var null|string */
		$current = $this->iterator->current();

		return $current;
	}
}

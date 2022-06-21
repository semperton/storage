<?php

declare(strict_types=1);

namespace Semperton\Storage;

/**
 * @param mixed $data
 */
function encode($data): string
{
	return json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
}
/**
 * @return mixed
 */
function decode(string $data)
{
	return json_decode($data, false, 512, JSON_THROW_ON_ERROR);
}

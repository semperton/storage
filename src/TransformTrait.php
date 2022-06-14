<?php

declare(strict_types=1);

namespace Semperton\Storage;

use InvalidArgumentException;
use stdClass;

trait TransformTrait
{
	/**
	 * @param mixed $data
	 */
	protected function encode($data): string
	{
		$json = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);

		if ($json === '' || $json[0] !== '{') {

			$type = gettype($data);
			throw new InvalidArgumentException("Only JSON objects are allowed - unable to encode data of type < $type >");
		}

		return $json;
	}

	/**
	 * @return mixed
	 */
	protected function decode(string $data)
	{
		return json_decode($data, false, 512, JSON_THROW_ON_ERROR);
	}

	/**
	 * @param mixed $value
	 * @return null|scalar|array|stdClass
	 */
	protected function convertJsonValue($value, string $type)
	{
		switch ($type) {
			case 'null':
				$value = null;
				break;
			case 'true':
				$value = true;
				break;
			case 'false':
				$value = false;
				break;
			case 'integer':
				$value = (int)$value;
				break;
			case 'real':
				$value = (float)$value;
				break;
			case 'text':
				$value = (string)$value;
				break;
			case 'array':
			case 'object':
				/** @var array|stdClass */
				$value = $this->decode((string)$value);
				break;
			default:
				throw new InvalidArgumentException("< $type > is not a valid JSON1 type");
		}

		return $value;
	}
}

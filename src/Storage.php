<?php

declare(strict_types=1);

namespace Semperton\Storage;

use InvalidArgumentException;
use Semperton\Database\ConnectionInterface;
use Semperton\Database\SQLiteConnection;
use Semperton\Query\QueryFactory;
use SQLite3;

abstract class Storage implements StorageInterface
{
	/** @var ConnectionInterface */
	protected $connection;

	/** @var QueryFactory */
	protected $queryFactory;

	/** @var int */
	protected $ivLength = 0;

	/** @var string */
	protected $cipherMethod;

	/** @var null|string */
	private $encryptionKey;

	// TODO: capability function
	// check pragma compile_options for ENABLE_JSON1 and ENABLE_UPDATE_DELETE_LIMIT

	public function __construct(
		string $filepath,
		?string $encryptionKey = null,
		string $cipherMethod = 'aes128'
	) {
		$this->connection = new SQLiteConnection($filepath, true, function (SQLite3 $sqlite) {

			// $sqlite->exec('pragma journal_mode = wal');
			$sqlite->exec('pragma application_id = 0x07111991');

			// TODO: set database flags
			// $sqlite->exec('pragma user_version = 1991');

			$sqlite->createFunction('encode', [$this, 'encode'], 1);
			$sqlite->createFunction('decode', [$this, 'decode'], 1, 2048); // SQLITE3_DETERMINISTIC
		});

		$this->queryFactory = new QueryFactory();

		$this->cipherMethod = $cipherMethod;

		if ($encryptionKey !== null) {

			$this->encryptionKey = $encryptionKey;

			if (!in_array($cipherMethod, openssl_get_cipher_methods(true))) {
				throw new InvalidArgumentException("Cipher method < $cipherMethod > is not supported");
			}

			$this->ivLength = (int)openssl_cipher_iv_length($cipherMethod);
		}
	}

	public function encode(string $data): string
	{
		if ($this->encryptionKey === null) {
			return $data;
		}

		$iv = openssl_random_pseudo_bytes($this->ivLength);

		return $iv . openssl_encrypt(
			$data,
			$this->cipherMethod,
			$this->encryptionKey,
			OPENSSL_RAW_DATA,
			$iv
		);
	}

	public function decode(string $data): string
	{
		if ($this->encryptionKey === null) {
			return $data;
		}

		$iv = substr($data, 0, $this->ivLength);

		return  openssl_decrypt(
			substr($data, $this->ivLength),
			$this->cipherMethod,
			$this->encryptionKey,
			OPENSSL_RAW_DATA,
			$iv
		);
	}

	public function get(string $collection): CollectionInterface
	{
		return new Collection($collection, $this, $this->connection, $this->queryFactory);
	}

	public function exists(string $collection): bool
	{
		$query = $this->queryFactory->select('sqlite_master');
		$query->fields([$query->raw('1')])->where('type', '=', 'table')->where('name', '=', $collection);

		$sql = $query->compile($params);

		$result = (int)$this->connection->fetchValue($sql, $params);

		return (bool)$result;
	}

	public function create(string $collection): CollectionInterface
	{
		$table = $this->queryFactory->quoteIdentifier($collection);
		$sql = 'create table if not exists ' . $table . ' (id integer primary key, document blob not null)';

		$this->connection->execute($sql);

		return $this->get($collection);
	}

	public function delete(string $collection): bool
	{
		$query = $this->queryFactory->drop($collection)->exists();
		$sql = $query->compile($params);

		return $this->connection->execute($sql, $params);
	}

	public function collections(): array
	{
		$query = $this->queryFactory->select('sqlite_master');
		$query->fields(['name'])->where('type', '=', 'table');

		$sql = $query->compile($params);

		$result = $this->connection->fetchColumn($sql, $params);

		/** @var string[] */
		return iterator_to_array($result);
	}
}

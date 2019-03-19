<?php

/**
 * A Cassandra (database) datastore.
 *
 * create keyspace sessionstore WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
 * 
 *
 * @package simpleSAMLphp
 */
class sspmod_cassandrastore_Store_CassandraStore extends SimpleSAML\Store {

	/**
	 * The Database object.
	 *
	 * @var DB
	 */
	public $db;



	/**
	 * Initialize the SQL datastore.
	 */
	protected function __construct() {

		$config = SimpleSAML\Configuration::getInstance();

		$keyspace 	= $config->getString('store.cassandra.keyspace');
		$nodes 		= $config->getArrayize('store.cassandra.nodes');
		$use_ssl    = $config->getBoolean('store.cassandra.use_ssl', false);
		$ssl_ca     = $config->getString('store.cassandra.ssl_ca', null);
		$username   = $config->getString('store.cassandra.username', null);
		$password   = $config->getString('store.cassandra.password', null);

		$cluster = \Cassandra::cluster()
				 ->withContactPoints(implode(',', $nodes))
				 ->withDefaultConsistency(\Cassandra::CONSISTENCY_LOCAL_QUORUM);

		if (isset($username) && isset($password)) {
			$cluster = $cluster->withCredentials($username, $password);
		}

		if ($use_ssl) {
			$ssl = \Cassandra::ssl()
				 ->withVerifyFlags(\Cassandra::VERIFY_PEER_CERT);
			if ($ssl_ca) {
				 $ssl_ca_files = explode(',', $ssl_ca);
				 $ssl = call_user_func_array(array($ssl, 'withTrustedCerts'), $ssl_ca_files);
			}
			$ssl = $ssl->build();
			$cluster = $cluster->withSSL($ssl);
		}

		$cluster = $cluster->build();
		$this->db = $cluster->connect($keyspace);
	}

	/**
	 * Convert long keys to something we can fit in the database table
	 */
	private function dbKey($key) {
		if (strlen($key) > 50) {
			$key = sha1($key);
		}
		return $key;
	}

	/**
	 * Retrieve a value from the datastore.
	 *
	 * @param string $type  The datatype.
	 * @param string $key  The key.
	 * @return mixed|NULL  The value.
	 */
	public function get($type, $key) {
		assert('is_string($type)');
		assert('is_string($key)');

		$key = $this->dbKey($key);

		$query = ' SELECT value FROM "session" WHERE type = :type AND key = :key';
		$params = array('type' => $type, 'key' => $key);

		// echo "<pre>About to perform a query \n"; print_r($query); echo "\n"; print_r($params); 
		// echo "\n\n";
		// debug_print_backtrace();
		// echo "\n------\n\n";
		// exit;

		// $result = $this->db->query($query, $params);

		$statement = new \Cassandra\SimpleStatement($query);
		$options = [
			'arguments' => $params,
			'consistency' => \Cassandra::CONSISTENCY_QUORUM,
		];
		try {
			$response = $this->db->execute($statement, $options);
		} catch (\Cassandra\Exception $e) {
			error_log("Received cassandra exception in get: " . $e);
			throw $e;
		}
		if (count($response) < 1) return null;
		$data = $response[0];

		$value = $data["value"];
		$value = urldecode($value);
		$value = unserialize($value);

		if ($value === FALSE) {
			return NULL;
		}
		return $value;
	}


	/**
	 * Save a value to the datastore.
	 *
	 * @param string $type  The datatype.
	 * @param string $key  The key.
	 * @param mixed $value  The value.
	 * @param int|NULL $expire  The expiration time (unix timestamp), or NULL if it never expires.
	 */
	public function set($type, $key, $value, $expire = NULL) {
		assert('is_string($type)');
		assert('is_string($key)');
		assert('is_null($expire) || (is_int($expire) && $expire > 2592000)');

		$key = $this->dbKey($key);

		$ttlstring = '';
		if ($expire !== NULL) {
			$ttl = intval($expire - time());
			if ($ttl < 0) {
				return;
			}
			$ttlstring = ' USING TTL ' . $ttl;
		}

		$value = serialize($value);
		$value = rawurlencode($value);


		$params = [
			"type" 	=> $type,
			"key"	=> $key,
			"value"	=> $value
		];
		$query = 'INSERT INTO "session" (type, key, value) VALUES (:type, :key, :value)' . $ttlstring;
		// echo "About to insert \n"; print_r($query); print_r($params); echo "\n\n";
		// $result = $this->db->query($query, $params);
		$statement = new \Cassandra\SimpleStatement($query);
		$options = [
			'arguments' => $params,
			'consistency' => \Cassandra::CONSISTENCY_QUORUM,
		];
		try {
			$this->db->execute($statement, $options);
		} catch (\Cassandra\Exception $e) {
			error_log("Received cassandra exception in set: " . $e);
			throw $e;
		}
	}


	/**
	 * Delete a value from the datastore.
	 *
	 * @param string $type  The datatype.
	 * @param string $key  The key.
	 */
	public function delete($type, $key) {
		assert('is_string($type)');
		assert('is_string($key)');

		$key = $this->dbKey($key);

		$params = [
			"type" 	=> $type,
			"key"	=> $key
		];
		$query = 'DELETE FROM "session" WHERE (type = :type AND key = :key)';
		// echo "About to delete \n"; print_r($query); print_r($params); echo "\n\n";
		// $result = $this->db->query($query, $params);
		$statement = new \Cassandra\SimpleStatement($query);
		$options = [
			'arguments' => $params,
			'consistency' => \Cassandra::CONSISTENCY_QUORUM,
		];
		try {
			$this->db->execute($statement, $options);
		} catch (\Cassandra\Exception $e) {
			error_log("Received cassandra exception in delete: " . $e);
			throw $e;
		}
	}

}

<?php

/**
 * A Cassandra (database) datastore.
 *
 * create keyspace sessionstore WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
 * 
 *
 * @package simpleSAMLphp
 */
class sspmod_cassandrastore_Store_CassandraStore extends SimpleSAML_Store {

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

		$config = SimpleSAML_Configuration::getInstance();

		$keyspace 	= $config->getString('store.cassandra.keyspace');
		$nodes 		= $config->getArrayize('store.cassandra.nodes');
		$use_ssl    = $config->getBoolean('store.cassandra.use_ssl', false);
		$username   = $config->getString('store.cassandra.username', null);
		$password   = $config->getString('store.cassandra.password', null);

		$hostprefix = '';
		if ($use_ssl) {
			$hostprefix = 'ssl://';
		}

		$nodelist = [];
		foreach ($nodes as $node) {
			$node_data = [
				'host' => $hostprefix . $node,
				'port' => 9042,
				'class'    => 'Cassandra\Connection\Stream',
			];
			if ($username and $password) {
				$node_data['username'] = $username;
				$node_data['password'] = $password;
			}
			$nodelist[] = $node_data;
		}

		$this->db = new \Cassandra\Connection($nodelist, $keyspace);
		$this->db->connect();

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

		if (strlen($key) > 50) {
			$key = sha1($key);
		}

		$query = ' SELECT value FROM "session" WHERE type = :type AND key = :key';
		$params = array('type' => $type, 'key' => $key);

		// echo "<pre>About to perform a query \n"; print_r($query); echo "\n"; print_r($params); 
		// echo "\n\n";
		// debug_print_backtrace();
		// echo "\n------\n\n";
		// exit;

		// $result = $this->db->query($query, $params);

		$response = $this->db->querySync($query, $params,
			\Cassandra\Request\Request::CONSISTENCY_QUORUM,
		    [
				'names_for_values' => true
		    ]);
		$result = $response->fetchAll();


		if (empty($result)) return null;
		if (count($result) < 1) return null;
		$data = $result[0];


	// echo var_dump($data); 

		$value = $data["value"];


		if (is_resource($value)) {
			$value = stream_get_contents($value);
		}

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

		if (strlen($key) > 50) {
			$key = sha1($key);
		}

		// if ($expire !== NULL) {
		// 	$expire = gmdate('Y-m-d H:i:s', $expire);
		// }

		$value = serialize($value);
		$value = rawurlencode($value);


		$params = [
			"type" 	=> $type,
			"key"	=> $key,
			"value"	=> $value
		];
		$query = 'INSERT INTO "session" (type, key, value) VALUES (:type, :key, :value)';
		// echo "About to insert \n"; print_r($query); print_r($params); echo "\n\n";
		// $result = $this->db->query($query, $params);
		$response = $this->db->querySync($query, $params,
			\Cassandra\Request\Request::CONSISTENCY_QUORUM,
		    [
				'names_for_values' => true
		    ]);

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

		if (strlen($key) > 50) {
			$key = sha1($key);
		}

		$params = [
			"type" 	=> $type,
			"key"	=> $key
		];
		$query = 'DELETE FROM "session" WHERE (type = :type AND key = :key)';
		// echo "About to delete \n"; print_r($query); print_r($params); echo "\n\n";
		// $result = $this->db->query($query, $params);
		$response = $this->db->querySync($query, $params,
			\Cassandra\Request\Request::CONSISTENCY_QUORUM,
		    [
				'names_for_values' => true
		    ]);

	}

}
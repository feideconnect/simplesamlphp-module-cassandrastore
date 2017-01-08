<?php


/**
 * A Cassandra (database) datastore for metadata
 */

class sspmod_cassandrastore_MetadataStore_CassandraMetadataStore extends SimpleSAML_Metadata_MetaDataStorageSource {
	/**
	 * The Database object.
     *
	 * @var DB
	 */
    public $db;


    /**
     * This is an associative array which stores the different metadata sets we have loaded.
     *
     * @var array
     */
    private $cachedMetadata = array();

	/**
	 * Initialize the SQL datastore.
	 */
     function __construct($sourceConfig)
     {
        assert('is_array($sourceConfig)');
		// $config = [];

        $config = SimpleSAML_Configuration::getInstance();

        $keyspace 	= $config->getString('metastore.cassandra.keyspace');
		$nodes 		= $config->getArrayize('metastore.cassandra.nodes');
		$use_ssl    = $config->getBoolean('metastore.cassandra.use_ssl', false);
		$ssl_ca     = $config->getString('metastore.cassandra.ssl_ca', null);
		$username   = $config->getString('metastore.cassandra.username', null);
		$password   = $config->getString('metastore.cassandra.password', null);

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
				$ssl = $ssl->withTrustedCerts($ssl_ca);
			}
			$ssl = $ssl->build();
			$cluster = $cluster->withSSL($ssl);
		}
		$cluster = $cluster->build();
		$this->db = $cluster->connect($keyspace);
	}

    /**
     * This function retrieves the given set of metadata. It will return an empty array if it is
     * unable to locate it.
     *
     * @param string $set The set of metadata we are retrieving.
     *
     * @return array An associative array with the metadata. Each element in the array is an entity, and the
     *         key is the entity id.
     */
    public function getMetadataSet($set)
    {
        if (array_key_exists($set, $this->cachedMetadata)) {
            return $this->cachedMetadata[$set];
        }

        if ($set !== 'saml20-idp-remote') {
            return [];
        }

        $metadataSet = [];
        $feed = $this->getFeed('edugain');
        foreach($feed AS $entityId => &$entity) {
            if ($entity['enabled'] && is_array($entity['metadata'])) {
                $entity['metadata']['entityid'] = $entityId;
                $metadataSet[$entityId] = $entity['metadata'];
            }
        }
        $this->cachedMetadata[$set] = $metadataSet;
        return $metadataSet;
    }


    /**
     * This function retrieves metadata for the given entity id in the given set of metadata.
     * It will return NULL if it is unable to locate the metadata.
     *
     * This class implements this function using the getMetadataSet-function. A subclass should
     * override this function if it doesn't implement the getMetadataSet function, or if the
     * implementation of getMetadataSet is slow.
     *
     * @param string $index The entityId or metaindex we are looking up.
     * @param string $set The set we are looking for metadata in.
     *
     * @return array An associative array with metadata for the given entity, or NULL if we are unable to
     *         locate the entity.
     */
    public function getMetaData($index, $set)
    {
        assert('is_string($index)');
        assert('isset($set)');

        if ($set !== 'saml20-idp-remote') {
            return null;
        }
        // echo "About to get entityid " . $index . "  " . $set; exit;
        return $this->getEntity('edugain', $index);
    }


	/**
	 * Save a value to the datastore.
	 *
	 * @param string $type  The datatype.
	 * @param string $key  The key.
	 * @param mixed $value  The value.
	 * @param int|NULL $expire  The expiration time (unix timestamp), or NULL if it never expires.
	 */
     public function insert($feed, $entityId, $metadata, $uimeta, $reg, $opUpdate = false) {

         assert('is_string($feed)');
         assert('is_string($entityId)');
         assert('is_array($metadata)');
         // $key = $this->dbKey($key);
         $metadataJSON = json_encode($metadata, true);
         $uimetaJSON = json_encode($uimeta, true);
         $query = 'INSERT INTO "entities" (feed, entityid, metadata, uimeta, reg, enabled, ' . ($opUpdate ? 'updated' : 'created') . ') VALUES (:feed, :entityid, :metadata, :uimeta, :reg, :enabled, :ts)';
         // echo "About to insert \n"; print_r($query); print_r($params); echo "\n\n";
         // $result = $this->db->query($query, $params);
         $statement = new \Cassandra\SimpleStatement($query);
         $params = [
			 'feed' => $feed,
			 'entityid' => $entityId,
			 'metadata' => $metadataJSON,
             'uimeta' => $uimetaJSON,
			 'reg' => $reg,
			 'enabled' => true,
			 'ts' => new \Cassandra\Timestamp(),
		 ];
         $options = new \Cassandra\ExecutionOptions([
             'arguments' => $params,
             'consistency' => \Cassandra::CONSISTENCY_QUORUM,
         ]);
         try {
             $this->db->execute($statement, $options);
         } catch (\Cassandra\Exception $e) {
             error_log("Received cassandra exception in set: " . $e);
             throw $e;
         }
     }


     public function getEntity($feed, $entityid) {
         assert('is_string($feed)');

         $query = 'SELECT entityid, feed, enabled, verification, metadata, uimeta, reg, created, updated FROM "entities" WHERE feed = :feed AND entityid = :entityid';
         $params = [
             'feed' => $feed,
             'entityid' => $entityid,
         ];
         $statement = new \Cassandra\SimpleStatement($query);
         $options = new \Cassandra\ExecutionOptions([
             'arguments' => $params,
             'consistency' => \Cassandra::CONSISTENCY_QUORUM,
         ]);
         try {
             $response = $this->db->execute($statement, $options);
         } catch (\Cassandra\Exception $e) {
             error_log("Received cassandra exception in get: " . $e);
             throw $e;
         }
         if (count($response) < 1) return null;
         $row = $response[0];

         $row['metadata'] = json_decode($row['metadata'], true);
         $row['uimeta'] = json_decode($row['uimeta'], true);
         $row['verification'] = json_decode($row['verification'], true);
         $row['created'] = (isset($row['created']) ? $row['created']->time() : null);
         $row['updated'] = (isset($row['updated']) ? $row['updated']->time() : null);
         if (!$row['enabled']) {
             return null;
         }
        //  echo '<pre>'; print_r($row); exit;
         return $row['metadata'];

     }

	public function getLogo($feed, $entityid) {
		$query = 'SELECT enabled, logo, logo_updated, logo_etag FROM "entities" WHERE feed = :feed AND entityid = :entityid';
		$params = [
			'feed' => $feed,
			'entityid' => $entityid,
		];
		$statement = new \Cassandra\SimpleStatement($query);
		$options = new \Cassandra\ExecutionOptions([
			'arguments' => $params,
			'consistency' => \Cassandra::CONSISTENCY_QUORUM,
		]);
		try {
			$response = $this->db->execute($statement, $options);
		} catch (\Cassandra\Exception $e) {
			error_log("Received cassandra exception in get: " . $e);
			throw $e;
		}
		if (count($response) < 1) return null;
		$row = $response[0];
		return $row;
	}


	public function getRegAuthUI($feed, $regauth) {
		assert('is_string($feed)');
		$query = 'SELECT entityid, enabled, verification, uimeta, reg, logo_etag, created, updated FROM "entities" WHERE feed = :feed ALLOW FILTERING';
		$params = array('feed' => $feed);
		$statement = new \Cassandra\SimpleStatement($query);
		$options = new \Cassandra\ExecutionOptions([
			'arguments' => $params,
			'consistency' => \Cassandra::CONSISTENCY_QUORUM,
		]);
		try {
			$response = $this->db->execute($statement, $options);
		} catch (\Cassandra\Exception $e) {
			error_log("Received cassandra exception in get: " . $e);
			throw $e;
		}
		$res = [];
		foreach($response AS $row) {
			if ($row['reg'] !== $regauth) { continue; }
			// $row['metadata'] = json_decode($row['metadata'], true);
			$row['uimeta'] = json_decode($row['uimeta'], true);
			$row['verification'] = json_decode($row['verification'], true);
			$row['created'] = (isset($row['created']) ? $row['created']->time() : null);
			$row['updated'] = (isset($row['updated']) ? $row['updated']->time() : null);
			$res[$row['entityid']] = $row;
		}
		return $res;
	}

     public function getFeed($feed) {
         assert('is_string($feed)');
         // $key = $this->dbKey($key);

         $query = 'SELECT entityid, feed, enabled, verification, metadata, uimeta, reg, logo_etag, created, updated FROM "entities" WHERE feed = :feed ALLOW FILTERING';
         $params = array('feed' => $feed);

        //  echo "<pre>About to perform a query \n"; print_r($query); echo "\n"; print_r($params);
        //  echo "\n\n";
        //  debug_print_backtrace();
        //  echo "\n------\n\n";
        //  exit;
         // $result = $this->db->query($query, $params);

         $statement = new \Cassandra\SimpleStatement($query);
         $options = new \Cassandra\ExecutionOptions([
             'arguments' => $params,
             'consistency' => \Cassandra::CONSISTENCY_QUORUM,
         ]);
         try {
             $response = $this->db->execute($statement, $options);
         } catch (\Cassandra\Exception $e) {
             error_log("Received cassandra exception in get: " . $e);
             throw $e;
         }
         // if (count($response) < 1) return [];
         $res = [];
         foreach($response AS $row) {
             $row['metadata'] = json_decode($row['metadata'], true);
             $row['uimeta'] = json_decode($row['uimeta'], true);
			 $row['logo_etag'] = $row['logo_etag'];
             $row['verification'] = json_decode($row['verification'], true);
             $row['created'] = (isset($row['created']) ? $row['created']->time() : null);
             $row['updated'] = (isset($row['updated']) ? $row['updated']->time() : null);
             $res[$row['entityid']] = $row;
         }
         //  print_r($res);
         return $res;

     }

	/**
	 * Delete a value from the datastore.
	 *
	 * @param string $feed  Feed.
	 * @param string $entityId  Entityid
	 */
	public function delete($feed, $entityId) {
		assert('is_string($feed)');
		assert('is_string($entityId)');


		$params = [
			"feed" 	=> $feed,
			"entityid"	=> $entityId
		];
		$query = 'DELETE FROM "entities" WHERE feed = :feed AND entityid = :entityid';
		// echo "About to delete \n"; print_r($query); print_r($params); echo "\n\n";
		// $result = $this->db->query($query, $params);
		$statement = new \Cassandra\SimpleStatement($query);
		$options = new \Cassandra\ExecutionOptions([
			'arguments' => $params,
			'consistency' => \Cassandra::CONSISTENCY_QUORUM,
		]);
		try {
			$this->db->execute($statement, $options);
		} catch (\Cassandra\Exception $e) {
			error_log("Received cassandra exception in delete: " . $e);
			throw $e;
		}
	}



	/**
	 * Delete a value from the datastore.
	 *
	 * @param string $feed  Feed.
	 * @param string $entityId  Entityid
	 */
	public function softDelete($feed, $entityId) {
		assert('is_string($feed)');
		assert('is_string($entityId)');

		$query = 'INSERT INTO "entities" (feed, entityid, enabled, updated) VALUES (:feed, :entityid, :enabled, :ts)';
		$statement = new \Cassandra\SimpleStatement($query);
		$params = [
			'feed' => $feed,
			'entityid' => $entityId,
			'enabled' => false,
			'ts' => new \Cassandra\Timestamp(),
		];
		$options = new \Cassandra\ExecutionOptions([
			'arguments' => $params,
			'consistency' => \Cassandra::CONSISTENCY_QUORUM,
		]);
		try {
			$this->db->execute($statement, $options);
		} catch (\Cassandra\Exception $e) {
			error_log("Received cassandra exception in set: " . $e);
			throw $e;
		}
	}

}

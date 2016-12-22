# simplesamlphp-cassandra-store

Cassandra Store for SimpleSAMLphp




## Using the session store


In config.php

```
'session.handler'       => 'cassandrastore:CassandraStore',
```


## Using the cassandra metadata store


In config.php

```
// Overridden in config.[prod/test].php
'metadata.sources' => array(
    array('type' => 'flatfile'),
    array('type' => 'cassandrastore:CassandraMetadataStore'),
),


'metastore.cassandra.keyspace' => $_ENV['CASSANDRA_KEYSPACE'],
'metastore.cassandra.nodes' => $_ENV['CASSANDRA_PORT_9042_TCP_ADDR'],
'metastore.cassandra.use_ssl' => false,
'metastore.cassandra.ssl_ca' => null,
'metastore.cassandra.username' => null,
'metastore.cassandra.password' => null,
```

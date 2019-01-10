<a id="ssl"></a>
# SSL Setup for Client-Server

SnappyData store supports Thrift protocol that provides functionality equivalent to JDBC/ODBC protocols and can be used to access the store from other languages that are not yet supported directly by SnappyData. In the command-line, SnappyData locators and servers accept the `-thrift-server-address` and -`thrift-server-port` arguments to start a Thrift server.

The thrift servers use the Thrift Compact Protocol by default which is not SSL enabled. When using the snappy-start-all.sh script, these properties can be specified in the *conf/locators* and *conf/servers* files in the product directory like any other locator/server properties.

In the *conf/locators* and *conf/servers* files, you need to add `-thrift-ssl` and required SSL setup in `-thrift-ssl-properties`. Refer to the [SnappyData thrift properties](property_description.md#thrift-properties) section for more information.

From the Snappy SQL shell:

```pre
snappy> connect client 'host:port;ssl=true;ssl-properties=...';
```
For JDBC use the same properties (without the "thrift-" prefix) like:

```pre
jdbc:snappydata://host:port/;ssl=true;ssl-properties=...
```
For example:

For a self-signed RSA certificate in keystore.jks, you can have the following configuration in the *conf/locators* and *conf/servers* files:

```pre
localhost -thrift-ssl=true -thrift-ssl-properties=keystore=keystore.jks,keystore-password=password,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256
```

Use the protocol/ciphers as per requirement. The corresponding setup on client-side can look like:

```pre
snappy> connect client 'localhost:1527;ssl=true;ssl-properties=truststore=keystore.jks,truststore-password=password,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256';
```

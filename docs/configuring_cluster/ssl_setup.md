# Enabling SSL Encryption in Different Socket Endpoints of SnappyData

SnappyData supports SSL encryption for the following:

*	[Client-server](#clientserversetup)
*	[Peer-to-peer (P2P)](#p2psetup)
*	[Spark layer](#sparksetup)
*	[Spark Jobserver](#jobserverssl)

SSL encryption for the three socket endpoints (P2P, client-server and Spark) must be [configured individually](#configureseparate) in their corresponding conf files. For jobserver, you must modify the **application.conf** file in the **spark-jobserver** repository (**/snappydata/spark-jobserver/job-server/resources/application.conf**) to activate the SSL communication.

!!! Note
	Properties that begin with `thrift` is for client-server, `javax.net.ssl` is for P2P, and `spark` is for Spark layer SSL settings.

<a id= contogether> </a>
## Enabling SSL Encryption Simultaneously for all the Socket Endpoints in SnappyData Cluster

Using the following configuration files, you can simultaneously enable SSL encryption for all the three socket endpoints (P2P, client-server, and park layer ssl encryption) in a SnappyData cluster.

!!! Note
	Currently, SSL encryption for all the socket endpoints must be configured separately. SnappyData intends to integrate Spark and P2P endpoints together in the future. Also for the jobserver, currently you cannot configure the SSL encryption through the startup properties or by using **user-specified conf** file. These supports are planned to be included in future releases.

In the following configuration files, a two node SnappyData cluster setup is done using the physical hosts **dev15** and **dev14**.

All examples given here includes one locator, one server, and one lead member in a SnappyData cluster configuration.

If you want to configure multiple locators, servers and lead members in a cluster, then ensure to copy all the SSL properties from each member configuration into the respective configuration files. 

For more information about each of these properties mentioned in the respective conf files, as well as for details on configuring multiple locators, servers, and lead members in a cluster,  refer to [SnappyData Configuration](configuring_cluster.md).

### Locator Configuration File (conf/locators)

```
# hostname locator <all ssl properties>
dev15 locators=dev15:10334 -ssl-enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-locatorKeyStore.keyfile> -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-locatorKeyStore.keyfile>  -J-Djavax.net.ssl.trustStorePassword=<password> -thrift-ssl=true -thrift-ssl-properties=keystore=<path-to-serverKeyStoreRSA.jks file>,keystore-password=<password>,,truststore=<path-to-trustStore.key file>,truststore-password=<password>,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256
```

### Server Configuration File (conf/servers)

```
# hostname locator <all ssl properties>
dev15 -locators=dev15:10334 -ssl-enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-serverKeyStore.key file> -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-serverKeyStore.key file> -J-Djavax.net.ssl.trustStorePassword=<password> -spark.ssl.enabled=true -spark.ssl.keyPassword=<password> -spark.ssl.keyStore=<path-to-serverKeyStore.key file> -spark.ssl.keyStorePassword=<password> -spark.ssl.trustStore=<path-to-serverKeyStore.key file> -spark.ssl.trustStorePassword=<password> -spark.ssl.protocol=TLS -client-port=1528 -thrift-ssl=true -thrift-ssl-properties=keystore=<path-to-serverKeyStoreRSA.jks file>,keystore-password=<password>,,truststore=<path-to-trustStore.key file>,truststore-password=<password>,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256

```

### Lead Configuration File (conf/leads)

```
# hostname locator <all ssl properties>
dev14 -locators=dev15:10334 -ssl-enabled=true  -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-leadKeyStore.key file>  -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-leadKeyStore.key file> -J-Djavax.net.ssl.trustStorePassword=<password> -spark.ssl.enabled=true -spark.ssl.keyPassword=<password> -spark.ssl.keyStore=<path-to-leadKeyStore.key file>  -spark.ssl.keyStorePassword=<password> -spark.ssl.trustStore=<path-to-leadKeyStore.key file> -spark.ssl.trustStorePassword=<password> -spark.ssl.protocol=TLS -client-port=1529
```

<a id= configureseparate> </a>
## Configuring SSL Encryption for Different Socket Endpoints in SnappyData Cluster

You can individually configure the SnappyData socket endpoints as per your requirements. This section provides the instructions to configure each of the socket endpoint.

*	[Configuring SSL encryption for client-server](#clientserversetup)
*	[Configuring SSL encryption for p2p](#p2psetup)
*	[Configuring SSL encryption for Spark](#sparksetup)


<a id= clientserversetup> </a>
### Configuring SSL Encryption for Client-server

SnappyData store supports the Thrift protocol that provides the functionality that is equivalent to JDBC/ODBC protocols and can be used to access the store from other languages that are not yet supported directly by SnappyData. In the command-line, SnappyData locators and servers accept the `-thrift-server-address` and `-thrift-server-port` arguments to start a Thrift server.
The Thrift servers use the **Thrift Compact Protocol** by default which is not SSL enabled. When using the `snappy-start-all.sh` script, these properties can be specified in the **conf/locators** and **conf/servers** files in the product directory like any other locator/server properties. For more information, refer to [SnappyData Configuration](configuring_cluster.md).

In the **conf/locators** and **conf/servers** files, you need to add `-thrift-ssl` and the required SSL setup in `-thrift-ssl-properties`. Refer to the [SnappyData thrift properties](../configuring_cluster/property_description.md#thrift-properties) section for more information.

In the following example, the SSL configuration for client-server is demonstrated alongwith the startup of SnappyData members with SSL encryption. 

#### Requirements

*	Configure SSL keypairs and certificates as needed for client and server. See [Generate Key Pairs and Certificates](https://gemfirexd.docs.pivotal.io/docs-gemfirexd/deploy_guide/Topics/security/ssl_keys.html#cadminsslkeys).
*	Ensure that all the SnappyData members use the same SSL boot parameters at startup.

#### Provider-Specific Configuration Files

This example uses keystores created by the Java keytool application to provide the proper credentials to the provider. To create the keystore and certificate for the client and server, run the following:

```
keytool -genkey -alias myserverkey -keystore serverKeyStoreRSA.jks -keyalg RSA
keytool -export -alias myserverkey -keystore serverKeyStoreRSA.jks  -rfc -file myServerRSA.cert
keytool -import -alias myserverkey -file myServerRSA.cert -keystore trustStore.key
```

The same keystore is used for SnappyData locator and server members as well as for client connection. You can enable SSL encryption for client-server connections by specifying the properties as the startup options for locator and server members. In the following example, the SSL encryption is enabled for communication between client-server.

#### Locator Configuration File (conf/locators)

```
# hostname locator <ssl properties for configuring SSL for SnappyData client-server connections>
 
localhost -thrift-ssl=true -thrift-ssl-properties=keystore=<path-to-serverKeyStoreRSA.jks file>,keystore-password=<password>,,truststore=<path-to-trustStore.key file>,truststore-password=<password>,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256

```

#### Server Configuration File (conf/servers)

```
# hostname locator <ssl properties for configuring SSL for SnappyData client-server connections>
 
localhost -thrift-ssl=true -thrift-ssl-properties=keystore=<path-to-serverKeyStoreRSA.jks file>,keystore-password=<password>,truststore=<path-to-trustStore.key file>,truststore-password=<password>,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256

```
Start the SnappyData cluster using `snappy-start-all.sh` script and perform operations either by using SnappyData shell or through JDBC connection. You can run the SnappyData quickstart example scripts for this. Refer to [SnappyData documentation](../quickstart/snappydataquick_start.md) for the same. 

Use the protocol/ciphers as per requirement. The corresponding setup on client-side can appear as follows:

```
snappy> connect client 'localhost:1527;ssl=true;ssl-properties=truststore=<path-to-trustStore.key file>,truststore-password=<password>,protocol=TLS,enabled-protocols=TLSv1:TLSv1.1:TLSv1.2,cipher-suites=TLS_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_256_CBC_SHA:TLS_RSA_WITH_AES_128_CBC_SHA256:TLS_RSA_WITH_AES_256_CBC_SHA256';

```

<a id= p2psetup> </a>
### Configuring SSL Encryption for P2P
In addition to using SSL for client-server connections, you can optionally configure SnappyData members to use SSL encryption and authorization for peer-to-peer(P2P) connections in the distributed system.

Peer SSL configuration is managed using `javax.net.ssl` system properties and the following SnappyData boot properties:

*	`ssl-enabled`
*	`ssl-protocols`
*	`ssl-ciphers`
*	`ssl-require-authentication`

The following sections provide an example that demonstrates the configuration and startup of SnappyData members with SSL encryption.

#### Requirements

To configure SSL for SnappyData peer connections:

*	Configure SSL keypairs and certificates as needed for each SnappyData member. See Generate Key Pairs and Certificates. 
*	Ensure that all SnappyData members use the same SSL boot parameters at startup.

#### Provider-Specific Configuration Files
This example uses keystores created by the Java keytool application to provide the proper credentials to the provider.

To create the keystore and certificate for the locator, run the following:

```
keytool -genkey -alias mySnappyLocator -keystore locatorKeyStore.key
keytool -export -alias mySnappyLocator -keystore locatorKeyStore.key -rfc -file myLocator.cert
```

You can use similar commands for a server member and a lead member respectively:

```
keytool -genkey -alias mySnappyServer -keystore serverKeyStore.key
keytool -export -alias mySnappyServer -keystore serverKeyStore.key -rfc -file myServer.cert
```

```
keytool -genkey -alias mySnappyLead -keystore leadKeyStore.key
keytool -export -alias mySnappyLead -keystore leadKeyStore.key -rfc -file myLead.cert
```

Each of the member's certificate are then imported into the other member's trust store.

For the server member:

```
keytool -import -alias mySnappyServer -file ./myServer.cert -keystore ./locatorKeyStore.key
keytool -import -alias mySnappyServer -file ./myServer.cert -keystore ./leadKeyStore.key
```

For the locator member:

```
keytool -import -alias mySnappyLocator -file ./myLocator.cert -keystore ./serverKeyStore.key
keytool -import -alias mySnappyLocator -file ./myLocator.cert -keystore ./leadKeyStore.key
```

For the lead member:

```
keytool -import -alias mySnappyLead -file ./myLead.cert -keystore ./locatorKeyStore.key
keytool -import -alias mySnappyLead -file ./myLead.cert -keystore ./serverKeyStore.key
```

You can enable SSL encryption for peer connections by specifying the properties as the startup options for each member. 

In the following example, SSL encryption is enabled for communication between the members.

#### Locator Configuration File (conf/locators)

```
# hostname locator <ssl properties for configuring SSL for SnappyData P2P connections>
localhost -ssl-enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-locatorKeyStore.key file> -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-locatorKeyStore.key file> -J-Djavax.net.ssl.trustStorePassword=<password>
```

#### Server Configuration File (conf/servers)

```
# hostname locator <ssl properties for configuring SSL for #SnappyData P2P connections>
 
localhost -locators=localhost:10334 -ssl-enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-serverKeyStore.keyfile>  -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-serverKeyStore.key file> -J-Djavax.net.ssl.trustStorePassword=<password> -client-port=1528

```
#### Lead Configuration File (conf/leads)

```
# hostname locator <ssl properties for configuring SSL for #SnappyData P2P connections>
 
localhost -locators=localhost:10334 -ssl-enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-leadKeyStore.key file> -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-leadKeyStore.key file>  -J-Djavax.net.ssl.trustStorePassword=<password> -client-port=1529

```

Start the SnappyData cluster using `snappy-start-all.sh` script and perform operations using the SnappyData shell, SnappyData job, and Smart Connector mode. You can run the SnappyData quickstart example scripts for this. Refer to [SnappyData documentation](../quickstart/snappydataquick_start.md) for more information.

<a id= sparksetup> </a>
### Configuring SSL Encryption for Spark layer

Spark layer  SSL configuration is managed using the following SnappyData boot properties:

*	`spark.ssl.enabled`
*	`spark.ssl.keyPassword`
*	`spark.ssl.keyStore`
*	`spark.ssl.keyStorePassword`
*	`spark.ssl.trustStore`
*	`spark.ssl.trustStorePassword`
*	`spark.ssl.protocol`

The following sections provide a simple example that demonstrates the configuration and startup of SnappyData members for enabling Spark layer for Wire Encryption.

#### Requirements

To configure SSL for Spark layer:

*	Configure SSL keypairs and certificates as needed for each SnappyData member. See [Generate Key Pairs and Certificates](https://gemfirexd.docs.pivotal.io/docs-gemfirexd/deploy_guide/Topics/security/ssl_keys.html#cadminsslkeys).
*	Ensure theat SnappyData locator, server, and lead members use the same SSL boot parameters at startup.

#### Provider-Specific Configuration Files

This example uses keystores created by the Java keytool application to provide the proper credentials to the provider.  On each node, create keystore files, certificates, and truststore files. Here, in this example, to create the keystore and certificate for the locator, the following were run:

```
keytool -genkey -alias mySnappyLocator -keystore locatorKeyStore.key
keytool -export -alias mySnappyLocator -keystore locatorKeyStore.key -rfc -file myLocator.cert

```

Similar commands were used for a server member and lead member respectively:

```
keytool -genkey -alias mySnappyServer -keystore serverKeyStore.key
keytool -export -alias mySnappyServer -keystore serverKeyStore.key -rfc -file myServer.cert
```

```
keytool -genkey -alias mySnappyLead -keystore leadKeyStore.key
keytool -export -alias mySnappyLead -keystore leadKeyStore.key -rfc -file myLead.cert
```

Each of the member's certificate was then imported into the other member's trust store.

For the locator member:
```
keytool -import -alias mySnappyLocator -file ./myLocator.cert -keystore ./serverKeyStore.key
keytool -import -alias mySnappyLocator -file ./myLocator.cert -keystore ./leadKeyStore.key
```

For the server member:
```
keytool -import -alias mySnappyServer -file ./myServer.cert -keystore ./locatorKeyStore.key
keytool -import -alias mySnappyServer -file ./myServer.cert -keystore ./leadKeyStore.key
```

For the lead member:
```
keytool -import -alias mySnappyLead -file ./myLead.cert -keystore ./locatorKeyStore.key
keytool -import -alias mySnappyLead -file ./myLead.cert -keystore ./serverKeyStore.key
```

You can enable SSL encryption for Spark layer by specifying the properties as the startup options for server and lead members. This example demonstrates how SSL encryption is enabled for a Spark layer connection.

#### Locator Configuration File (conf/locators)

```
# hostname locator <ssl properties for configuring SSL for SnappyData Spark layer>
localhost -ssl-enabled=true -spark.ssl.enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-locatorKeyStore.key file> -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-locatorKeyStore.key file> -J-Djavax.net.ssl.trustStorePassword=<password> 
-spark.ssl.keyPassword=<password> -spark.ssl.keyStore=<path-to-locatorKeyStore.key file> -spark.ssl.keyStorePassword=<password> -spark.ssl.trustStore=<path-to-locatorKeyStore.key file> -spark.ssl.trustStorePassword=<password> -spark.ssl.protocol=TLS 
```

#### Server Configuration File (conf/servers)

```
# hostname locator <ssl properties for configuring SSL for #SnappyData spark layer>
 
localhost -locators=localhost:10334 -ssl-enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-serverKeyStore.key file> -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-serverKeyStore.key file> -J-Djavax.net.ssl.trustStorePassword=<password>
-spark.ssl.enabled=true -spark.ssl.keyPassword=<password> -spark.ssl.keyStore=<path-to-serverKeyStore.key file> -spark.ssl.keyStorePassword=<password> -spark.ssl.trustStore=<path-to-serverKeyStore.key file> -spark.ssl.trustStorePassword=<password> -spark.ssl.protocol=TLS 
```

#### Lead Configuration File (conf/leads)

```
# hostname locator <ssl properties for configuring SSL for #SnappyData spark layer>
localhost -locators=localhost:10334  -ssl-enabled=true -J-Djavax.net.ssl.keyStoreType=jks -J-Djavax.net.ssl.keyStore=<path-to-leadKeyStore.key file> -J-Djavax.net.ssl.keyStorePassword=<password> -J-Djavax.net.ssl.trustStore=<path-to-leadKeyStore.key file> -J-Djavax.net.ssl.trustStorePassword=<password>
-spark.ssl.enabled=true -spark.ssl.keyPassword=<password> -spark.ssl.keyStore=<path-to-leadKeyStore.key file> -spark.ssl.keyStorePassword=<password> -spark.ssl.trustStore=/<path-to-leadKeyStore.key file> -spark.ssl.trustStorePassword=<password> -spark.ssl.protocol=TLS 
Start the SnappyData cluster using snappy-start-all.sh script and perform operations using SnappyData shell, SnappyData  job, and Smart Connector mode.
```

You can run the SnappyData quickstart example scripts for this. Refer to [SnappyData documentation](../quickstart/snappydataquick_start.md) for more information.

<a id= jobserverssl> </a>
### Configuring SSL Encryption for Spark Job Server

To activate SSL communication in the Spark job server, you must set these flags in your **application.conf** file (Section 'spray.can.server'). Later, you must either compile the jobserver repository after changing the settings in this conf file or compile the product after modifying this conf file.

```
ssl-encryption = on
  # absolute path to keystore file
  keystore = "/some/path/keystore.jks"
  keystorePW = "changeit"
```
!!! Note
	Currently you cannot configure SSL encryption for Spark job server through the startup properties or by using user-specified conf file. This is planned to be supported in the future releases.

#### Requirements

A keystore that contains the server certificate. 

#### Provider-Specific Configuration Files
This example uses keystores created by the Java keytool application to provide the proper credentials to the provider. You must create keystore files, certificates, and truststore files on each node of the SnappyData cluster.

1.	Create a keystore file.

		keytool -genkey -alias localhost -keyalg RSA -keystore jobserverkeystoreRSA.jks

2.	Create a certificate.

		keytool -export -alias localhost -keystore jobserverkeystoreRSA.jks -rfc -file jobserverkeystore.cert

3.	Create a truststore file.

		keytool -import -noprompt -alias localhost -file jobserverkeystore.cert -keystore jobservertrustStore.key

4.	Create one truststore file that contains the public keys from all certificates.
5.	Log on to one host and import the truststore file for that host and copy the <all_jks> file to the other nodes in your cluster, and repeat the keytool command on each node in case of multi host cluster.
6.	Modify the **application.conf** file under **spark-jobserver **repository (**/snappydata/spark-jobserver/job-server/resources/application.conf**) to activate server authentication and SSL communication.
7.	Modify the section **spray.can.server** in this file for the following properties:

         ssl-encryption = on
          # absolute path to keystore file
          keystore = "/path to/jobserverkeystoreRSA.jks"
          keystorePW = "<password>"
     
8.	Use Curl commands to submit a sample job to SSL enabled Spark job server:

		curl -k --data-binary @/path/to/jarfile/snappydata-store-scala-tests-0.1.0-SNAPSHOT-tests.jar 	https://localhost:8090/jars/myapp;
		curl -k -d  "" 'https://localhost:8090/jobs?appName=myapp&classPath=io.snappydata.hydra.northwind.CreateTableJob';

### Code Snippet for Sample CreateTableJob (snappyjob)

```
package io.snappydata.hydra.northwind
 
import java.io.{File, FileOutputStream, PrintWriter}
 
import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}
 
import scala.util.{Failure, Success, Try}
 
 
class CreateTableJob extends SnappySQLJob {
 override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
   val pw = new PrintWriter(new FileOutputStream(new File("CreateTableJob.out"), true));
   def getCurrentDirectory = new java.io.File(".").getCanonicalPath
 
   Try {
     val snc = snSession.sqlContext
     snc.sql("create table t1 (id int) using column")
     snc.sql("select * from t1").show()
     snc.sql("insert into t1 values (1)").show()
     snc.sql("select * from t1").show()
     snc.sql("insert into t1 values (2)").show()
     snc.sql("select * from t1").show()
     snc.sql(" update t1 set id=3 where id=1").show()
     snc.sql("select * from t1").show()
     pw.println("table craete/inserted/update successfully....")
   } match {
     case Success(v) => pw.close()
       s"See ${getCurrentDirectory}/CreateTableJob.out"
     case Failure(e) => pw.close();
       throw e;
   }
 }
 
 override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}



```
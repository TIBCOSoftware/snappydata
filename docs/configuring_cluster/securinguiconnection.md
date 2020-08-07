# Securing TIBCO ComputeDB Monitoring Console Connection

You can secure the TIBCO ComputeDB Monitoring Console with SSL authentication so that the UI can be accessed only over HTTPS. The following configurations are needed to set up SSL enabled connections for TIBCO ComputeDB Monitoring Console:

**To set up SSL enabled connections for TIBCO ComputeDB Monitoring Console:**

1. Make sure that you have valid SSL certificate imported into truststore.
2. Provide the following spark configuration in the conf/lead files:

		localhost -spark.ssl.enabled=true -spark.ssl.protocol=<ssl-protocol> -spark.ssl.enabledAlgorithms=<comma-separated-list-of-ciphers> -spark.ssl.keyPassword=<key-password> -spark.ssl.keyStore=<path-to-key-store> -spark.ssl.keyStorePassword=<key-store-password> -spark.ssl.keyStoreType=<key-store-type> -spark.ssl.trustStore=<path-to-trust-store> -spark.ssl.trustStorePassword=<trust-store-password> -spark.ssl.trustStoreType=<trust-store-type>

	!!!Note
		 - If using TLS SSL protocol, the enabledAlgorithms can be TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
     	- Store types could be JKS or PKCS12.

3.	Launch the Snappy cluster.</br>
	`./sbin/snappy-start-all.sh` 
4.	Launch the TIBCO ComputeDB Monitoring Console on your web browser. You are directed to the HTTPS site.

!!!Note
	You are automatically redirected to HTTPS (on port 5450) even if the TIBCO ComputeDB Monitoring Console is accessed with HTTP protocol.

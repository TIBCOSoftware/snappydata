# Securing SnappyData Pulse UI Connection

You can secure the SnappyData Pulse UI with SSL authentication, so that the UI can be accessed only over HTTPS. The following configurations are needed to set up SSL enabled connections for SnappyData Pulse UI:

**To set up SSL enabled connections for SnappyData Pulse UI:**

1. Make sure that you have valid SSL certificate imported into truststore.
2. Provide the following spark configuration in the leads conf files:

		localhost-spark.ssl.enabled=true \
          		-spark.ssl.protocol=<ssl-protocol> \
          		-spark.ssl.enabledAlgorithms=<comma-separated-list-of-ciphers> \
          		-spark.ssl.keyPassword=<key-password> \
          		-spark.ssl.keyStore=<path-to-key-store> \
          		-spark.ssl.keyStorePassword=<key-store-password> \
          		-spark.ssl.keyStoreType=<key-store-type> \
          		-spark.ssl.trustStore=<path-to-trust-store> \
          		-spark.ssl.trustStorePassword=<trust-store-type> \
          		-spark.ssl.trustStoreType=<trust-store-type>

	!!!Note
		 - If using TLS SSL protocol, the enabledAlgorithms can be TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA
     	- Store types could be JKS or PKCS12.

3.	Launch the Snappy cluster.
	`./sbin/snappy-start-all.sh` 
4.	Launch the Snappy Pulse UI in your web browser. You are directed to the HTTPS site.

!!!Note
	Users are automatically redirected to HTTPS (on port 5450) even if the SnappyData Pulse UI is accessed with HTTP protocol.

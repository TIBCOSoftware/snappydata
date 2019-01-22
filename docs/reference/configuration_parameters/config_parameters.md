# Configuration Properties

You use JDBC connection properties, connection boot properties, and Java system properties to configure SnappyData members and connections.

* [Property Types](#property-types)

* [Property Prefixes](#property-prefixes)

* [Using Non-ASCII Strings in SnappyData Property Files](#non-ascii-strings)

* [List of Property Names](#property-names)


<a id="property-types"></a>
### Property Types

SnappyData configuration properties are divided into the following property types:

-   **Connection properties**. Connection properties configure the features of a SnappyData member or a SnappyData client connection when you start or connect to a SnappyData member. You can define connection properties directly in the JDBC connection URL (or in the "connect" command in an interactive SnappyData session). You can also define connection properties in the `gemfirexd.properties` file or as Java system properties. For example, use -J-D*property_name*=*property_value* when you execute a `snappy` utility. Or, use the JAVA_ARGS environment variable to define a Java system property for an interactive `snappy` session (JAVA_ARGS="-D*property_name*=*property_value*"). 
   
	!!! Note
		You must add a prefix to certain connection property names in order to specify those properties as Java system properties. See [Property Prefixes](#property-prefixes).

    Connection properties can be further categorized as either *boot properties* or *client properties*:

	-   **Boot properties**. A boot connection property configures features of a SnappyData member, and can only be applied along with the first connection that starts a SnappyData member. Boot properties have no effect when they are specified on connections to a member after the member has started. Boot properties have no effect when they are specified on a thin client connection.

	-   **Client properties**. A client connection property configures features of the client connection itself and can be used with the JDBC thin client drive (for example, using a JDBC thin client connection URL or the `connect client` command from an interactive `snappy` session).

-   **System properties**. Certain SnappyData configuration properties *must* be specified either as Java system properties (using -J-D*property_name*=*property_value* with a `snappy` utility or setting JAVA_ARGS="-D*property_name*=*property_value*" for an interactive `snappy` session). You cannot define these properties in a JDBC URL connection. Many of SnappyData system properties affect features of the SnappyData member at boot time and can be optionally defined in the gemfirexd.properties file. See the property description to determine whether or not a system property can be defined in gemfirexd.properties.

    The names of SnappyData system properties always include the `snappydata.` prefix. For example, all properties that configure LDAP server information for user authentication must be specified as Java system properties, rather than JDBC properties, when you boot a server.

Certain properties have additional behaviors or restrictions. See the individual property descriptions for more information.


<a id="property-prefixes"></a>
### Property Prefixes

You must add a prefix to connection and boot property names when you define those properties as Java system properties. The **Prefix** row in each property table lists a prefix value (`snappydata.` or `gemfire.`) when one is required. Do not use an indicated prefix when you specify the property in a connection string.

If no prefix is specified, use only the indicated property name in all circumstances. For example, use "host-data" whether you define this property in gemfirexd.properties, as a Java system property, or as a property definition for FabricServer.

<a id="non-ascii-strings"></a>
### Using Non-ASCII Strings in SnappyData Property Files

You can specify Unicode (non-ASCII) characters in SnappyData property files by using a `\uXXXX` escape sequence. For a supplementary character, you need two escape sequences, one for each of the two UTF-16 code units. The XXXX denotes the 4 hexadecimal digits for the value of the UTF-16 code unit. For example, a properties file might have the following entries:

```pre
s1=hello there
s2=\u3053\u3093\u306b\u3061\u306f
pre

For example, in `gemfirexd.properties`, you might write:

```pre
log-file=my\u00df.log
```

to indicate the desired property definition of `log-file=my.log`.

If you have edited and saved the file in a non-ASCII encoding, you can convert it to ASCII with the `native2ascii` tool included in your Oracle Java distribution. For example, you might want to do this when editing a properties file in Shift_JIS, a popular Japanese encoding.

<a id="property-names"></a>
### List of Property Names
Below is the list of all the configuration properties and links for each property reference page.

- [ack-severe-alert-threshold](ack-severe-alert-threshold.md)

- [ack-wait-threshold](ack-wait-threshold.md)

- [archive-disk-space-limit](archive-disk-space-limit.md)

- [archive-file-size-limit](archive-file-size-limit.md)

- [bind-address](bind-address.md)

- [enable-network-partition-detection](enable-network-partition-detection.md)

- [enable-stats](enable-stats.md)

- [enable-time-statistics](enable-time-statistics.md)

- [enable-timestats](enable-timestats.md)

- [enforce-unique-host](enforce-unique-host.md)

- [init-scripts](init-scripts.md)

- [load-balance](load-balance.md)

- [locators](locators.md)

- [log-file](log-file.md)

- [log-level](log-level.md)

- [member-timeout](member-timeout.md)

- [membership-port-range](membership-port-range.md)

- [password](password.md)

- [read-timeout](read-timeout.md)

- [redundancy-zone](redundancy-zone.md)

- [secondary-locators](secondary-locators.md)

- [skip-constraint-checks](skip-constraint-checks.md)

- [skip-locks](skip-locks.md)

- [socket-buffer-size](socket-buffer-size.md)

- [socket-lease-time](socket-lease-time.md)

- [gemfirexd.datadictionary.allow-startup-errors](snappydata.datadictionary.allow-startup-errors.md)

- [gemfirexd.default-startup-recovery-delay](snappydata.default-startup-recovery-delay.md)

- [snappy.history](snappy.history.md)

- [gemfirexd.max-lock-wait](snappydata.max-lock-wait.md)

- [gemfirexd.query-cancellation-interval](snappydata.query-cancellation-interval.md)

- [gemfirexd.query-timeout](snappydata.query-timeout.md)

- [ssl](ssl.md)

- [start-locator](start-locator.md)

- [statistic-archive-file](statistic-archive-file.md)

- [statistic-sample-rate](statistic-sample-rate.md)

- [statistic-sampling-enabled](statistic-sampling-enabled.md)

- [sync-commits](sync-commits.md)

- [sys-disk-dir](sys-disk-dir.md)

- [user](user.md)

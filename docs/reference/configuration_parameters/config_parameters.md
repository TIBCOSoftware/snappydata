# Configuration Properties

You use JDBC connection properties, connection boot properties, and Java system properties to configure SnappyData members and connections.

* [Property Types](#property-types)

* [Property Prefixes](#property-prefixes)

* [Using Non-ASCII Strings in SnappyData Property Files](#non-ascii-strings)

* [List of Property Names](#property-names)


<a id="property-types"></a>
#### Property Types

SnappyData configuration properties are divided into the following property types:

-   **Connection properties**. Connection properties configure the features of a SnappyData member or a SnappyData client connection when you start or connect to a SnappyData member. You can define connection properties directly in the JDBC connection URL (or in the "connect" command in an interactive gfxd session). You can also define connection properties in the <span class="ph filepath">gemfirexd.properties</span> file or as Java system properties. For example, use -J-D*property\_name*=*property\_value* when you execute a `snappy-shell` utility. Or, use the JAVA\_ARGS environment variable to define a Java system property for an interactive `snappy-shell` session (JAVA\_ARGS="-D*property\_name*=*property\_value*"). 
   
	!!! Note:
		You must add a prefix to certain connection property names in order to specify those properties as Java system properties. See [Property Prefixes](#property-prefixes). </p>

    Connection properties can be further categorized as either *boot properties* or *client properties*:

    -   **Boot properties**. A boot connection property configures features of a SnappyData member, and can only be applied with the first connection that starts a SnappyData member. You can specify boot properties when you start a SnappyData member using the FabricServer API or snappy-shell command, when you make the first connection to an embedded SnappyData member with the JDBC embedded driver, or when you use the `connect peer` command from an interactive `snappy-shell` session to start a peer client. Boot properties have no effect when they are specified on connections to a member after the member has started. Boot properties have no effect when they are specified on a thin client connection.

    -   For example, the [server-groups](server-groups.md) property configures a data store's server groups when the data store boots. If you specify `server-groups` on any subsequent connection to the data store, the server group configuration is not changed.
    
    -   **Client properties**. A client connection property configures features of the client connection itself, and can be used with the JDBC thin client drive (for example, using a JDBC thin client connection URL or the `connect client` command from an interactive `snappy-shell` session).

-   **System properties**. Certain SnappyData configuration properties *must* be specified either as Java system properties (using -J-D*property\_name*=*property\_value* with a `snappy-shell` utility, or setting JAVA\_ARGS="-D*property\_name*=*property\_value*" for an interactive `snappy-shell` session). You cannot define these properties in a JDBC URL connection. Many of SnappyData system properties affect features of the SnappyData member at boot time, and can be optionally defined in the <span class="ph filepath">gemfirexd.properties</span> file. See the property description to determine whether or not a system property can be defined in <span class="ph filepath">gemfirexd.properties</span>.

    The names of SnappyData system properties always include the "snappydata." prefix. For example, all properties that configure LDAP server information for user authentication must be specified as Java system properties, rather than JDBC properties, when you boot a server.

Certain properties have additional behaviors or restrictions. See the individual property descriptions for more information.


<a id="property-prefixes"></a>
#### Property Prefixes

You must add a prefix to connection and boot property names when you define those properties as Java system properties. The **Prefix** row in each property table lists a prefix value ("snappydata." or "gemfire.") when one is required. Do not use an indicated prefix when you specify the property in a connection string or with the FabricServer API.

If no prefix is specified, use only the indicated property name in all circumstances. For example, use "host-data" whether you define this property in <span class="ph filepath">gemfirexd.properties</span>, as a Java system property, or as a property definition for FabricServer.

<a id="non-ascii-strings"></a>
#### Using Non-ASCII Strings in SnappyData Property Files

You can specify Unicode (non-ASCII) characters in SnappyData property files by using a `\uXXXX` escape sequence. For a supplementary character, you need two escape sequences, one for each of the two UTF-16 code units. The XXXX denotes the 4 hexadecimal digits for the value of the UTF-16 code unit. For example, a properties file might have the following entries:

``` pre
s1=hello there
s2=\u3053\u3093\u306b\u3061\u306f
```

For example, in `gemfirexd.properties`, you might write:

``` pre
log-file=my\u00df.log
```

to indicate the desired property definition of `log-file=myÃ.log`.

If you have edited and saved the file in a non-ASCII encoding, you can convert it to ASCII with the `native2ascii` tool included in your Oracle Java distribution. For example, you might want to do this when editing a properties file in Shift\_JIS, a popular Japanese encoding.

<a id="jdbc_connection_attributes__section_8E297DC67DCE4C97AD5B47F069714C73"></a>

<a id="property-names"></a>
#### List of Property Names
Below is the list to all the configuration properties and links for each property reference page.

- [ack-severe-alert-threshold](ack-severe-alert-threshold.md)

- [ack-wait-threshold](ack-wait-threshold.md)

- [archive-disk-space-limit](archive-disk-space-limit.md)

- [archive-file-size-limit](archive-file-size-limit.md)

- [async-distribution-timeout](async-distribution-timeout.md)

- [async-max-queue-size](async-max-queue-size.md)

- [async-queue-timeout](async-queue-timeout.md)

- [auth-provider](auth-provider.md)

- [bind-address](bind-address.md)

- [conserve-sockets](conserve-sockets.md)

- [default-recovery-delay](default-recovery-delay.md)

- [departure-correlation-window](departure-correlation-window.md)

- [disable-auto-reconnect](disable-auto-reconnect.md)

- [disable-streaming](disable-streaming.md)

- [disable-tcp](disable-tcp.md)

- [distributed-system-id](distributed-system-id.md)

- [drdaID](drdaID.md)

- [enable-network-partition-detection](enable-network-partition-detection.md)

- [enable-stats](enable-stats.md)

- [enable-time-statistics](enable-time-statistics.md)

- [enable-timestats](enable-timestats.md)

- [enforce-unique-host](enforce-unique-host.md)

- [gemfire.DISKSPACE_WARNING_INTERVAL](gemfire.DISKSPACE_WARNING_INTERVAL.md)

- [max_view_log_size](max_view_log_size.md)

- [gemfire.LOCK_MAX_TIMEOUT](gemfire.LOCK_MAX_TIMEOUT.md)

- [gemfire.preAllocateDisk](gemfire.preAllocateDisk.md)

- [gemfire.preAllocateIF](gemfire.preAllocateIF.md)

- [gemfire.syncWrites](gemfire.syncWrites.md)

- [gemfire.tx-disable-batching](gemfire.tx-disable-batching.md)

- [host-data](host-data.md)

- [init-scripts](init-scripts.md)

- [jmx-manager](jmx-manager.md)

- [jmx-manager-access-file](jmx-manager-access-file.md)

- [jmx-manager-hostname-for-clients](jmx-manager-hostname-for-clients.md)

- [jmx-manager-http-port](jmx-manager-http-port.md)

- [jmx-manager-password-file](jmx-manager-password-file.md)

- [jmx-manager-port](jmx-manager-port.md)

- [jmx-manager-ssl](jmx-manager-ssl.md)

- [jmx-manager-start](jmx-manager-start.md)

- [jmx-manager-update-rate](jmx-manager-update-rate.md)

- [keepalive-count](keepalive-count.md)

- [keepalive-idle](keepalive-idle.md)

- [keepalive-interval](keepalive-interval.md)

- [load-balance](load-balance.md)

- [locators](locators.md)

- [log-disk-space-limit](log-disk-space-limit.md)

- [log-file](log-file.md)

- [log-file-size-limit](log-file-size-limit.md)

- [log-level](log-level.md)

- [max-num-reconnect-tries](max-num-reconnect-tries.md)

- [max-wait-time-reconnect](max-wait-time-reconnect.md)

- [mcast-address](mcast-address.md)

- [mcast-flow-control](mcast-flow-control.md)

- [mcast-port](mcast-port.md)

- [mcast-recv-buffer-size](mcast-recv-buffer-size.md)

- [mcast-send-buffer-size](mcast-send-buffer-size.md)

- [mcast-ttl](mcast-ttl.md)

- [member-timeout](member-timeout.md)

- [membership-port-range](membership-port-range.md)

- [password](password.md)

- [persist-dd](persist-dd.md)

- [prefer-netserver-ipaddress](prefer-netserver-ipaddress.md)

- [read-timeout](read-timeout.md)

- [redundancy-zone](redundancy-zone.md)

- [secondary-locators](secondary-locators.md)

- [security-log-file](security-log-file.md)

- [security-log-level](security-log-level.md)

- [security-peer-verifymember-timeout](security-peer-verifymember-timeout.md)

- [server-auth-provider](server-auth-provider.md)

- [server-bind-address](server-bind-address.md)

- [server-groups](server-groups.md)

- [shutdown](shutdown.md)

- [single-hop-enabled](single-hop-enabled.md)

- [skip-constraint-checks](skip-constraint-checks.md)

- [skip-listeners](skip-listeners.md)

- [skip-locks](skip-locks.md)

- [socket-buffer-size](socket-buffer-size.md)

- [socket-lease-time](socket-lease-time.md)

- [snappydata.auth-ldap-search-base](snappydata.auth-ldap-search-base.md)

- [snappydata.auth-ldap-search-dn](snappydata.auth-ldap-search-dn.md)

- [snappydata.auth-ldap-search-filter](snappydata.auth-ldap-search-filter.md)

- [snappydata.auth-ldap-search-pw](snappydata.auth-ldap-search-pw.md)

- [snappydata.auth-ldap-server](snappydata.auth-ldap-server.md)

- [snappydata.authz-default-connection-mode](snappydata.authz-default-connection-mode.md)

- [snappydata.authz-full-access-users](snappydata.authz-full-access-users.md)

- [snappydata.authz-read-only-access-users](snappydata.authz-read-only-access-users.md)

- [snappydata.sql-authorization](snappydata.sql-authorization.md)

- [snappydata.datadictionary.allow-startup-errors](snappydata.datadictionary.allow-startup-errors.md)

- [snappydata.default-startup-recovery-delay](snappydata.default-startup-recovery-delay.md)

- [snappydata.drda.host](snappydata.drda.host.md)

- [snappydata.drda.keepAlive](snappydata.drda.keepAlive.md)

- [snappydata.drda.logConnections](snappydata.drda.logConnections.md)

- [snappydata.drda.maxThreads](snappydata.drda.maxThreads.md)

- [snappydata.drda.minThreads](snappydata.drda.minThreads.md)

- [snappydata.drda.portNumber](snappydata.drda.portNumber.md)

- [snappydata.drda.securityMechanism](snappydata.drda.securityMechanism.md)

- [snappydata.drda.sslMode](snappydata.drda.sslMode.md)

- [snappydata.drda.streamOutBufferSize](snappydata.drda.streamOutBufferSize.md)

- [snappydata.drda.timeSlice](snappydata.drda.timeSlice.md)

- [snappydata.drda.trace](snappydata.drda.trace.md)

- [snappydata.drda.traceAll](snappydata.drda.traceAll.md)

- [snappydata.drda.traceDirectory](snappydata.drda.traceDirectory.md)

- [snappydata.history](snappydata.history.md)

- [snappydata.language.permissionsCacheSize](snappydata.language.permissionsCacheSize.md)

- [snappydata.language.spsCacheSize](snappydata.language.spsCacheSize.md)

- [snappydata.language.statementCacheSize](snappydata.language.statementCacheSize.md)

- [snappydata.language.tableDescriptorCacheSize](snappydata.language.tableDescriptorCacheSize.md)

- [snappydata.max-lock-wait](snappydata.max-lock-wait.md)

- [gemfirexd.properties](gemfirexd.properties.md)

- [snappydata.query-cancellation-interval](snappydata.query-cancellation-interval.md)

- [snappydata.query-timeout](snappydata.query-timeout.md)

- [snappydata.security. Properties (prefix)](snappydata.security.md)

- [snappydata.client.single-hop-max-connections](snappydata.client.single-hop-max-connections.md)

- [snappydata.stream.error.logSeverityLevel](snappydata.stream.error.logSeverityLevel.md)

- [snappydata.storage.tempDirectory](snappydata.storage.tempDirectory.md)

- [snappydata.system.home](snappydata.system.home.md)

- [ssl](ssl.md)

- [ssl-ciphers](ssl-ciphers.md)

- [ssl-enabled](ssl-enabled.md)

- [ssl-protocols](ssl-protocols.md)

- [ssl-require-authentication](ssl-require-authentication.md)

- [start-locator](start-locator.md)

- [statistic-archive-file](statistic-archive-file.md)

- [statistic-sample-rate](statistic-sample-rate.md)

- [statistic-sampling-enabled](statistic-sampling-enabled.md)

- [sync-commits](sync-commits.md)

- [sys-disk-dir](sys-disk-dir.md)

- [tcp-port](tcp-port.md)

- [udp-fragment-size](udp-fragment-size.md)

- [udp-recv-buffer-size](udp-recv-buffer-size.md)

- [udp-send-buffer-size](udp-send-buffer-size.md)

- [user](user.md)


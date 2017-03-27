# run
Connects to a SnappyData distributed system and executes the contents of a SQL command file. All commands in the specified file must be compatible with the interactive gfxd shell.

##Syntax

``` pre
snappy-shell run -file=<path or URL>
     [-auth-provider=<name>]
     [-bind-address=<address>]
     [-client-bind-address=<address>]
     [-client-port=<port>]
     [-encoding=<charset>]
     [-extra-conn-props=<properties>] 
     [-help] 
     [-ignore-errors]
     [-J-D<property=value>]
     [-locators=<adresses>]
     [-mcast-address=<address>]
     [-mcast-port=<port>]
     [-password[=<password>]]
     [-path=<path>]
     [-user=<username>]
```

This table describes options for the `snappy-shell run` command. Default values are used if you do not specify an option.

|Option||Description|
|-|-|
|-file|</br>The local path of a SQL command file to execute, or a URL that links to the SQL command file. All commands in the specified file must be compatible with the interactive gfxd shell.</br>This argument is required.|
|-auth-provider|Sets the authentication provider to use for peer-to-peer connections as well as client-server connections. Valid values are BUILTIN and LDAP. All other members of the SnappyData distributed system must use the same authentication provider and user definitions. If you omit this option, the connection uses no authentication mechanism. See <a href="../../deploy_guide/Topics/security/security_chapter.html#concept_6CD7D8A0C41E4BD2B0A5A8A6B192537F" class="xref" title="You secure a SnappyData deployment by configuring user authentication and SQL authorization, and enabling encryption between members using SSL/TLS.">Configuring Security</a>.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default gfxd uses the hostname, or localhost if the hostname points to a local loopback address.|
|-client-bind-address|</br>The hostname or IP address on which a SnappyData locator listens for client connections. The default is &quot;localhost.&quot; </br>Use this option with `-client-port` to attach to a SnappyData cluster as a thin client and perform the command.|
|-client-port|</br>The port on which a SnappyData locator listens for client connections. The default is 1527.</br>Use this option with `-client-bind-address` to attach to a SnappyData cluster as a thin client and perform the command.|
|-encoding|The character set encoding of the SQL script file (`-file` argument). The default is UTF-8. Other possible values are: US-ASCII, ISO-8859-1, UTF-8, UTF-16BE, UTF-16LE, UTF-16. See the <a href="http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html" class="xref">java.nio.charset.Charset</a> reference for more information.|
|-extra-conn-props|</br>A semicolon-separated list of properties to use when connecting to the SnappyData distributed system.|
|-help, --help|</br>Display the help message for this snappy-shell command.|
|-ignore-errors|Include this option to ignore any errors that may occur while executing statements in the file, and continue executing the remaining statements. If you omit this option, then gfxd immediately terminates the script's execution if an exception occurs.|
|-J-D&lt;property=value&gt;|Sets Java system property to the specified value.|
|-locators|</br>The list of locators as comma-separated host[port] values, used to discover other members of the distributed system.</br>Using `-locators` creates a peer client member to execute the snappy-shell command.|
|-mcast-address|</br>The multicast address used to discover other members of the distributed system. This value is used only when the `-locators` option is not specified. The default multicast address is 239.192.81.1. </br>Use this option with `-mcast-port` to attach to a SnappyData cluster as a peer client and perform the command.|
|-mcast-port|</br>The multicast port used to communicate with other members of the distributed system. If zero, multicast is not used for member discovery (specify `-locators` instead). This value is used only if the `-locators` option is not specified.</br>Valid values are in the range 0–65535, with a default value of 10334.</br>Use this option with `-mcast-address` to attach to a SnappyData cluster as a peer client and perform the command.|
|-password|</br>If the servers or locators have been configured to use authentication, this option specifies the password for the user (specified with the -user option) to use for booting the server and joining the distributed system.</br>The password value is optional. If you omit the password, gfxd prompts you to enter a password from the console.|
|-path|Configures the working directory for any other SQL command files executed from within the script. The <code class="ph codeph">-path</code> entry is prepended to any SQL script file name executed that the script executes in a <a href="run.html#rtoolsijcomref28886" class="xref noPageCitation" title="Treats the value of the string as a valid file name, and redirects gfxd processing to read from that file until it ends or an exit command is executed.">run</a> command.|
|-user||If the servers or locators have been configured to use authentication, this option specifies the user name to use for booting the server and joining the distributed system.|

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_F763D37B83E54D828B8572FF3192C67F"></a>
##Description

Specify one of these pairs of options with the command to connect to a SnappyData Distributed system and execute a SQL command file:

-   Use both `-client-bind-address` and `-client-port` to connect to a SnappyData cluster as a thin client and perform the command.
-   Use both `mcast-port` and `-mcast-address`, or use the `-locators` property to connect to a SnappyData cluster as a peer client and perform the command.

The `-file` argument specifies the location of the SQL script file to execute. If the script file itself calls other script files using `run 'filename'`, also consider using the `-path` option to specify the location of the embedded script files. If an exception occurs while executing the script, GFXD immediately stops executing script commands, unless you include the `-ignore-errors` option.

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_216B020FDAC94495AB858096A117F350"></a>
## Examples

This command connects to a SnappyData network server running on myserver:1234 and executes commands in the <span class="ph filepath">ToursDB\_schema.sql</span> file:

``` pre
snappy-shell run -file=c:\gemfirexd\quickstart\ToursDB_schema.sql
     -client-bind-address=myserver -client-port=1234
```

This command executes the <span class="ph filepath">loadTables.sql</span> script, which calls dependent scripts such as <span class="ph filepath">loadCOUNTRIES.sql</span>, <span class="ph filepath">loadCITIES.sql</span>, and so forth. If you execute this command outside of the directory in which the dependent scripts are located, you must specify the working directory using the `-path` option. For example:

``` pre
snappy-shell run -file=c:\gemfirexd\quickstart\loadTables.sql
     -path=c:\gemfirexd\quickstart
     -client-bind-address=myserver -client-port=1234
```

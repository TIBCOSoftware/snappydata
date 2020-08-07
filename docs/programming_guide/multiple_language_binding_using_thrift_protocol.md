# Multiple Language Binding using Thrift Protocol

TIBCO ComputeDB provides support for Apache Thrift protocol which enables users to access the cluster from other languages that are not supported directly by TIBCO ComputeDB.
Thrift allows efficient and reliable communication across programming languages like Java, Python, PHP, Ruby, Elixir, Perl and other languages. For more information on Thrift, refer to the [Apache Thrift documentation](https://thrift.apache.org/).

The JDBC driver for TIBCO ComputeDB that uses the `jdbc:snappydata://` URL schema, now uses Thrift for underlying protocol. The older URL scheme for RowStore `jdbc:gemfirexd://` continues to use the deprecated DRDA protocol.

Likewise, locators and servers in TIBCO ComputeDB now default to starting up thrift servers and when started in RowStore mode (`snappy-start-all.sh rowstore`) the DRDA servers are started as before.

To explicitly start a DRDA server in TIBCO ComputeDB, you can use the `-drda-server-address` and `-drda-server-port` options for the **bind address** and **port** respectively. Likewise, to explicitly start a Thrift server in RowStore mode, you can use the `-thrift-server-address` and `-thrift-server-port` options.

Refer to the following documents for information on support provided by TIBCO ComputeDB:</br>

 * [**About TIBCO ComputeDB Thrift**]( https://github.com/TIBCO ComputeDBInc/snappydata/blob/branch-0.9/cluster/README-thrift.md): Contains detailed information about the feature and its capabilities.

 * [**The Thrift Interface Definition Language (IDL)**](https://github.com/SnappyDataInc/snappy-store/blob/branch-1.5.4/gemfirexd/shared/src/main/java/io/snappydata/thrift/common/snappydata.thrift): This is a Thrift interface definition file for the TIBCO ComputeDB service.

 * [**Example**](https://github.com/SnappyDataInc/snappy-store/blob/branch-1.5.4/gemfirexd/tools/src/test/java/io/snappydata/app/TestThrift.java):
 Example of the Thrift definitions using the TIBCO ComputeDB Thrift IDL.

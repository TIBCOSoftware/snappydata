# Release Notes 

The SnappyData team is pleased to announce the availability of SnappyData Release 1.0.0 (GA) of the platform.

<heading2> New Features/Fixed Issues</heading2>

- Fully compatible with Apache Spark 2.1.1

- Mutability support for column store (SNAP-1389):

  - UPDATE and DELETE operations are now supported on column tables.

- ALTER TABLE support for row table (SNAP-1326).

- Security Support (available in the [Enterprise Edition](http://www.snappydata.io/download)):  This release introduces cluster security with authentication and authorisation based on LDAP mechanism. It will be extended to other mechanisms in future releases. (SNAP-1656, SNAP-1813).

- DEB and RPM installers (distProduct target in source build).

- Support for setting scheduler pools using the set command.

- Multi-node cluster now boots up quickly as background start of server processes is enabled by default.

- Pulse Console:  SnappyData Pulse has been enhanced to be more useful to both developers and operations personnel (SNAP-1890, SNAP-1792). Improvements include

  - Ability to sort members list based on members type.

  - Added new UI view named SnappyData Member Details Page which includes, among other things, latest logs.

  - Added members Heap and Off-Heap memory usage details along with their storage and execution splits.

- Users can specify streaming batch interval when submitting a stream job via conf/snappy-job.sh (SNAP-1948).

- Row tables now support LONG, SHORT, TINYINT and BYTE datatypes (SNAP-1722).

- The history file for snappy shell has been renamed from .gfxd.history to .snappy.history. You may copy your existing ~/.gfxd.history to ~/.snappy.history to be able to access your historical snappy shell commands.

<heading2> Performance Enhancements</heading2>

- Performance enhancements with dictionary decoder when dictionary is large. (SNAP-1877)
  - Using a consistent sort for pushed down predicates so that different sessions do not end up creating different generated code.
  - Reduced the size of generated code.
- Indexed cursors in decoders to improve heavily filtered queries (SNAP-1936)
- Performance improvements in Smart Connector mode, specially with queries on tables with wide schema (SNAP-1363, SNAP-1699)
- Several other performance improvements.

<heading2> Select bug fixes and performance related fixes</heading2>

There have been numerous bug fixes done as part of this release. Some of these are included below. For a more comprehensive list, see [ReleaseNotes](https://github.com/SnappyDataInc/snappydata/blob/master/ReleaseNotes.txt).

| Issue | Description |
|------|--------|
|SNAP-1756|Fixed data inconsistency issues when a new node is joining the cluster and at the same time write operations are going on.|
|SNAP -1377</br>SNAP -902|The product internally does retries on redundant copy of partitions on the event of a node failure|
|SNAP -1893|Fixed the wrong status of locators on restarts. After cluster restart, snappy-status-all.sh used to show locators in waiting state even when the actual status changed to running|
|SNAP -1426|Fixed the SnappyData Pulse freezing when loading data sets|
|SNAP -1688</br>SNAP-1798|More accurate accounting of execution and storage memory|
|SNAP&nbsp;-1714|Corrected case-sensitivity handling for query API calls|

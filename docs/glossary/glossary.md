[A](#a)  |  [B](#b)  |  [C](#c)  |  [D](#d)  |  [E](#e)  |  [F](#f)  |  [G](#g)  |  [H](#h)  |  [I](#i)  |  [J](#j)  |  [K](#k)  |[L](#l)  |  [M](#m)  |  [N](#n)  |  [O](#o)  |  [P](#p)  |  [Q](#q)  |  [R](#r)  |  [S](#s)  |  [T](#t)  |  [U](#u)  |  [V](#v)  |  [W](#w)  |  [X](#x)  |  [Y](#y)  |  [Z](#z)
<hr>

<a id="a"></a>
<glossary> A</glossary></br>

<hr>
<a id="b"></a> 
<glossary>B</glossary></br>

<hr>
<a id="c"></a> 
<glossary>C</glossary></br>

<hr>
<a id="d"></a> 
<glossary>D</glossary></br>

<hr>
<a id="e"></a> 
<glossary>E</glossary></br>

<hr>
<a id="f"></a> 
<glossary>F</glossary></br>

<hr>
<a id="g"></a> 
<glossary>G</glossary></br>

<hr>
<a id="h"></a> 
<glossary>H</glossary></br>

<hr>
<a id="i"></a> 
<glossary>I</glossary></br>

<hr>
<a id="j"></a> 
<glossary>J</glossary></br>

<hr>
<a id="k"></a> 
<glossary>K</glossary></br>

<hr>
<a id="l"></a> 
<glossary>L</glossary></br>

<glossaryterm>
Locator
</glossaryterm>

<glossarytext>
A locator facilitates discovery of all members in a distributed system. This is a component that maintain a registry of all peer members in the distributed system at any given moment. Though typically started as a separate process (with redundancy for HA), a locator can also be embedded in any peer member (like a server). This opens a TCP port and all new members connect to this process to get initial membership information for the distributed system.
</glossarytext>

<glossaryterm>
Lead Node/ Lead
</glossaryterm>

<glossarytext>
Lead Nodes act as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance, but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.
</glossarytext>

<hr>
<a id="m"></a> 
<glossary>M</glossary></br>

<hr>
<a id="n"></a> 
<glossary>N</glossary></br>

<hr>
<a id="o"></a> 
<glossary>O</glossary></br>

<hr>
<a id="p"></a> 
<glossary>P</glossary></br>

<glossaryterm>Partitioned Table</glossaryterm>
<glossarytext>A table that manages large volumes of data by partitioning it into manageable chunks and distributing it across all the servers in its hosting server groups. Partitioning attributes, including the partitioning strategy can be specified by supplying a PARTITION BY clause in a CREATE TABLE statement. See also replicated table and <mark>Add Link</mark> partitioning strategy <mark>Add Link</mark>.</glossarytext>

<glossaryterm>Partitioning Strategy</glossaryterm>
<glossarytext>The policy used to determine the specific bucket for a field in a partitioned table. SnappyData currently only supports horizontal partitioning , so an entire row is stored in the same bucket. You can hash-partition a table based on its primary key or on an internally-generated unique row id if the table has no primary key. Other partitioning strategies can be specified in the PARTITION BY clause in a CREATE TABLE statement. The strategies that are supported by SnappyData include hash-partitioning on columns other than the primary key, range-partitioning , and list-partitioning.</glossarytext>

<hr>
<a id="q"></a> 
<glossary>Q</glossary></br>

<hr>
<a id="r"></a> 
<glossary>R</glossary></br>
<glossaryterm>Resilient Distributed Dataset (RDD)</glossaryterm>
<glossarytext> </glossarytext>

<glossaryterm>Replicated Table</glossaryterm>
<glossarytext>A table that keeps a copy of its entire dataset locally on every data store in its server groups. SnappyData creates replicated tables by default if you do not specify a PARTITION BY clause. See also partitioned table.<mark>Add Link</mark></glossarytext>

<hr>
<a id="s"></a> 
<glossary>S</glossary></br>

<glossaryterm>
Server
</glossaryterm>

<glossarytext>
A JVM started with the `snappy-shell server` command, or any JVM that calls the `FabricServer.start` method. A SnappyData server may or may not also be a data store, and may or may not also be a network server.
</glossarytext>

<hr>
<a id="t"></a> 
<glossary>T</glossary></br>

<hr>
<a id="u"></a> 
<glossary>U</glossary></br>

<hr>
<a id="v"></a> 
<glossary>V</glossary></br>

<hr>
<a id="w"></a> 
<glossary>W</glossary></br>

<hr>
<a id="x"></a> 
<glossary>X</glossary></br>

<hr>
<a id="y"></a> 
<glossary>Y</glossary></br>

<hr>
<a id="z"></a> 
<glossary>Z</glossary></br>

</br>

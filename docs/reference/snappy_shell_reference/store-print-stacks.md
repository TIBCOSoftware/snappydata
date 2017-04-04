# print-stacks

Prints a stack dump of SnappyData member processes.

##Syntax

``` pre
snappy print-stacks
  [-all-threads] [<filename>] [-J-D<vmprop>=<prop-value>]
  [-mcast-port=<port>]
  [-mcast-address=<address>]
  [-locators=<addresses>]
  [-bind-address=<addr>]
  [-<prop-name>=<prop-value>]*
```

The table describes options for `snappy print-stacks`. If no multicast or locator options are specified on the command-line, then the command uses the <span class="ph filepath">gemfirexd.properties</span> file (if available) to determine the distributed system to which it should connect.

|Option|Description|
|-|-|
|-all-threads|By default this command attempts to remove idle SnappyData threads from the stack dump. Include `-all-threads` to include idle threads in the dump.|
|[&lt;filename&gt;]|An optional filename to store the stack dumps. See also <a href="../system_procedures/dump-stacks.html#reference_A7533A4A873D48FBAB05A67DD5CC7F66" class="xref" title="Writes thread stacks, locks, and transaction states to the SnappyData log file. You can write stack information either for the current SnappyData member or for all SnappyData members in the distributed system.">SYS.DUMP\_STACKS</a> for information about appending stack dump information to the SnappyData log file.|
|-mcast-port|Multicast port used to communicate with other members of the distributed system. If zero, multicast is not used for member discovery (specify `-locators` instead).</br>Valid values are in the range 0–65535, with a default value of 10334.|
|-mcast-address|Multicast address used to discover other members of the distributed system. This value is used only if the `-locators` option is not specified.</br>The default multicast address is 239.192.81.1.|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default `gfxd` uses the hostname, or localhost if the hostname points to a local loopback address.|
|-&lt;prop-name&gt;=&lt;prop-value&gt;|Any other SnappyData distributed system property.|

<a id="reference_13F8B5AFCD9049E380715D2EF0E33BDC__section_050663B03C0A4C42B07B4C5F69EAC95D"></a>
##Example

The following command prints the stack dump of all SnappyData processes to standard out:

``` pre
$ snappy print-stacks -all-threads

Connecting to distributed system: mcast=/239.192.81.1:10334
--- dump of stack for member ward(1940)<v0>:48434 ------------------------------------------------------------------------------
GfxdLocalLockService@2916ab8[gfxd-ddl-lock-service]: PRINTSTACKS

snappy-ddl-lock-service:   0 tokens, 0 locks held
__VMID_LS:   0 tokens, 0 locks held
__PRLS:   0 tokens, 0 locks held


TX states:

Full Thread Dump:

"Pooled Message Processor 1" Id=73 WAITING
    at sun.misc.Unsafe.park(Native Method)
    at java.util.concurrent.locks.LockSupport.park(LockSupport.java:283)
    at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.awaitFulfill(SynchronousQueueNoSpin.java:451)
    at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin$TransferStack.transfer(SynchronousQueueNoSpin.java:352)
    at com.gemstone.java.util.concurrent.SynchronousQueueNoSpin.take(SynchronousQueueNoSpin.java:886)
    at java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:957)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:917)
    at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:728)
    at com.gemstone.gemfire.distributed.internal.DistributionManager$5$1.run(DistributionManager.java:1023)
    at java.lang.Thread.run(Thread.java:695)

"Pooled High Priority Message Processor 2" Id=72 RUNNABLE
    at sun.management.ThreadImpl.dumpThreads0(Native Method)
    at sun.management.ThreadImpl.dumpAllThreads(ThreadImpl.java:433)
    at com.pivotal.snappydata.internal.engine.locks.GfxdLocalLockService.generateThreadDump(GfxdLocalLockService.java:373)
    at com.pivotal.snappydata.internal.engine.locks.GfxdLocalLockService.dumpAllRWLocks(GfxdLocalLockService.java:362)
    at com.pivotal.snappydata.internal.engine.store.RegionEntryUtils$1.printStacks(RegionEntryUtils.java:1360)
    at com.gemstone.gemfire.internal.OSProcess.zipStacks(OSProcess.java:482)
    at com.gemstone.gemfire.distributed.internal.HighPriorityAckedMessage.process(HighPriorityAckedMessage.java:174)
    at com.gemstone.gemfire.distributed.internal.DistributionMessage.scheduleAction(DistributionMessage.java:415)
    at com.gemstone.gemfire.distributed.internal.DistributionMessage$1.run(DistributionMessage.java:483)
    at java.util.concurrent.ThreadPoolExecutor$Worker.runTask(ThreadPoolExecutor.java:895)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:918)
    at com.gemstone.gemfire.distributed.internal.DistributionManager.runUntilShutdown(DistributionManager.java:728)
    at com.gemstone.gemfire.distributed.internal.DistributionManager$6$1.run(DistributionManager.java:1060)
    at java.lang.Thread.run(Thread.java:695)

    Number of locked synchronizers = 1
    - java.util.concurrent.locks.ReentrantLock$NonfairSync@775c024c
[...]
```

Include a filename argument to store the stack dump in a file instead of writing to standard out:

``` pre
$ snappy print-stacks -all-threads gfxd-stack-dump.txt

Connecting to distributed system: mcast=/239.192.81.1:10334
1 stack dumps written to gfxd-stack-dump.txt
```

See also <a href="../system_procedures/dump-stacks.html#reference_A7533A4A873D48FBAB05A67DD5CC7F66" class="xref" title="Writes thread stacks, locks, and transaction states to the SnappyData log file. You can write stack information either for the current SnappyData member or for all SnappyData members in the distributed system.">SYS.DUMP\_STACKS</a> for information about appending stack dump information to the SnappyData log file.



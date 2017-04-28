# Managing How Data is Written to Disk

You can configure SnappyData to write immediately to disk and you may be able to modify your operating system behavior to perform buffer flushes more frequently.

Typically, SnappyData writes disk data into the operating system's disk buffers and the operating system periodically flushes the buffers to disk. Increasing the frequency of writes to disk decreases the likelihood of data loss from application or machine crashes, but it impacts performance.

<a id="disk_buffer_flushes__section_448348BD28B14F478D81CC2EDC6C7049"></a>
## Modifying Disk Flushes for the Operating System

You may be able to change the operating system settings for periodic flushes. You may also be able to perform explicit disk flushes from your application code. For information on these options, see your operating system's documentation. For example, in Linux you can change the disk flush interval by modifying the setting `/proc/sys/vm/dirty_expire_centiseconds`. It defaults to 30 seconds. To alter this setting, see the Linux documentation for `dirty_expire_centiseconds`.

<a id="disk_buffer_flushes__section_D1068505581A43EE8395DBE97297C60F"></a>

## Modifying SnappyData to Flush Buffers on Disk Writes

You can have SnappyData flush the disk buffers on every disk write. Do this by setting the system property <mark>gemfire.syncWrites TO BE CONFIRMED</mark> to true at the command line when you start your SnappyData member. You can modify this setting only when you start a member. When this property is set, SnappyData uses a Java `RandomAccessFile` with the flags "rwd", which causes every file update to be written synchronously to the storage device. This only guarantees your data if your disk stores are on a local device. See the Java documentation for `java.IO.RandomAccessFile`.

Configure this property when you start a SnappyData member:

``` pre
snappy server start -J-Dgemfire.syncWrites=true
```

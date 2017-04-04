# Disk Store Directories

When you create a disk store, you can optionally specify the location of directories where SnappyData stores persistence-related files.

SnappyData generates disk store artifacts in a directory that it chooses in the following order:

1.  If you provide absolute directory paths in the See [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md) command, SnappyData uses the paths as-is. You must ensure that the proper directory structure exists.

2.  If you provide relative directory paths to See [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md), or you do not specify any directories, then the path resolution is done in this order:
    1.  If the <mark>[sys-disk-dir]()</mark> boot property specifies a directory, the disk store path is resolved relative to that directory, or that directory is used as the directory for persistence.

    2.  If `sys-disk-dir` is not set, SnappyData uses the directory from which the system was started (the current directory) for path resolution, or it uses that directory as the persistence directory.

You can optionally specify the maximum amount of combined oplog files that can be stored in a particular disk store directory. However, keep in mind that the specified maximum must be large enough to accommodate the disk store files and maintain enough free space to avoid triggering disk full warnings. SnappyData preallocates space for oplog files when you create the disk store, and the command will fail if any directory specifies size limit that is too small. See [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md) for more information.



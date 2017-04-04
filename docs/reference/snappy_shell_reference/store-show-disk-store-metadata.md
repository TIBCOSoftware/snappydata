# show-disk-store-metadata

Display the disk store metadata for a specified disk store directory.

## Syntax


``` pre
snappy-shell show-disk-store-metadata <disk-store> <directory>+
  [-mcast-port=<port>]
  [-mcast-address=<address>]
  [-locators=<addresses>] 
        [-bind-address=<address>] 
  [-<prop-name>=<prop-value>]*
```

The table describes options and arguments for gfxd show-disk-store-metadata. If no multicast or locator options are specified on the command-line, the command uses the <span class="ph filepath">gemfirexd.properties</span> file (if available) to determine the distributed system to which it should connect.


| Option | Description |
|----------------------|---------------------------------------------|
|`<disk-store>`        |(Required.) Specifies the name of a disk store for which you want to display metadata. The disk store must be offline.        |
|`<directory>`        |(Required.) Specifies one or more disk store file directories.        |
|-mcast-port        |Multicast port used to communicate with other members of the distributed system. If zero, multicast is not used for member discovery (specify `-locators` instead). </br> Valid values are in the range 0–65535, with a default value of 10334.        |
|-mcast-address        |Multicast address used to discover other members of the distributed system. This value is used only if the `-locators` option is not specified. </br> The default multicast address is 239.192.81.1.        |
|-locators        |List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values.        |
|-bind-address        |The address to which this peer binds for receiving peer-to-peer messages. By default gfxd uses the hostname, or localhost if the hostname points to a local loopback address.        |
|-prop-name        |Any other SnappyData distributed system property.        |

<a id="reference_FF886BB14E5949B79E47AC334D23EEE5__section_373A5D6CDE984CC49A03632C63252F2E"></a>
## Description


This command displays metadata information for all available disk stores in the specified directory. On Linux RPM installations, the disk store directory and files are owned by the non-interactive `gemfirexd` user. You must use `sudo -u gemfirexd` to execute the disk store command.

<a id="reference_FF886BB14E5949B79E47AC334D23EEE5__section_AFA4A7ACB7BA4CD58E33C8711B607AAD"></a>

## Example

This command displays metadata for all disk stores in a single directory:

``` pre
sudo -u gemfirexd gfxd show-disk-store-metadata GFXD-DEFAULT-DISKSTORE /var/opt/snappydata/rowstore/server
Disk Store ID: cb70441cf00f43f7-93b3cc5fe1ecd2d9
Regions in the disk store:
  /__UUID_PERSIST
    lru=lru-entry-count
    lruAction=overflow-to-disk
    lruLimit=100
    concurrencyLevel=16
    initialCapacity=16
    loadFactor=0.75
    statisticsEnabled=false
    drId=9
    isBucket=false
    clearEntryId=0
    MyInitializingID=<null>
    MyPersistentID=</192.168.125.137:/var/opt/snappydata/rowstore/server/. created at timestamp 1360363313694 version 0 diskStoreId cb70441cf00f43f7-93b3cc5fe1ecd2d9 name null>
    onlineMembers:
    offlineMembers:
    equalsMembers:
```

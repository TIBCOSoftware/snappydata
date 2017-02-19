## Starting Members Individually using Command Line (without scripts)

Instead of starting SnappyData members using SSH scripts, they can be individually configured and started using the command line. 

```bash 
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334

$ bin/snappy-shell locator stop
$ bin/snappy-shell server stop
```
  
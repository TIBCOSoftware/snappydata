## Starting Members Individually using Command Line (without scripts)

Instead of starting SnappyData members using SSH scripts, they can be individually configured and started using the command line. 

```bash 
$ bin/snappy locator start  -dir=/node-a/locator1 
$ bin/snappy server start  -dir=/node-b/server1  -locators:localhost:10334

$ bin/snappy locator stop
$ bin/snappy server stop
```
  

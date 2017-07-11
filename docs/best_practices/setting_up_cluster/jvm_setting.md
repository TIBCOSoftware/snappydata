<a id="jvm-settings"></a>
## JVM Settings for SnappyData Smart Connector mode and Local mode 

For SnappyData Smart Connector mode and local mode, we recommend the following JVM settings for optimal performance:

```
-XX:-DontCompileHugeMethods -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=4k
```
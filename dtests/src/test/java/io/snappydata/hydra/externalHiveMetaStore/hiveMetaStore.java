package io.snappydata.hydra.externalHiveMetaStore;

import io.snappydata.hydra.cluster.SnappyTest;

public class hiveMetaStore extends SnappyTest
{
    public static void HydraTask_Wait() throws InterruptedException  {
        int count = 0;
        while(count <= 10) {
            count++;
            Thread.sleep(20000);
         }
    }
}
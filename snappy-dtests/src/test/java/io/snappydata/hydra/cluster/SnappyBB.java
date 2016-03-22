package io.snappydata.hydra.cluster;

import hydra.blackboard.Blackboard;

/**
 * Created by swati on 22/3/16.
 */
public class SnappyBB extends Blackboard {
    //Blackboard creation variables
    static String SNAPPY_BB_NAME = "Snappy_Blackboard";
    static String SNAPPY_BB_TYPE = "RMI";

    public static SnappyBB bbInstance = null;

    public static synchronized SnappyBB getBB() {
        if (bbInstance == null) {
            bbInstance = new SnappyBB(SNAPPY_BB_NAME, SNAPPY_BB_TYPE);
        }
        return bbInstance;
    }

    public SnappyBB() {
    }

    public SnappyBB(String name, String type) {
        super(name, type, SnappyBB.class);
    }

    public static int serversStarted;
    public static int locatorsStarted;
    public static int leadsStarted;
}

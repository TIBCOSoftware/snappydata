package io.snappydata.hydra.cluster;

import hydra.blackboard.Blackboard;

/**
 * Holds ports for snappy network servers. e.g. server, locator
 */
public class SnappyNetworkServerBB extends Blackboard {

    private static SnappyNetworkServerBB blackboard;

    public static int connectionCount;

    /**
     * Zero-arg constructor for remote method invocations.
     */
    public SnappyNetworkServerBB() {
    }

    /**
     * Creates a blackboard using the specified name and transport type.
     */
    public SnappyNetworkServerBB(String name, String type) {
        super(name, type, SnappyNetworkServerBB.class);
    }

    /**
     * Creates a blackboard.
     */
    public static synchronized SnappyNetworkServerBB getBB() {
        if (blackboard == null) {
            blackboard =
                    new SnappyNetworkServerBB("NetworkServerBlackboard", "rmi");
        }
        return blackboard;
    }
}
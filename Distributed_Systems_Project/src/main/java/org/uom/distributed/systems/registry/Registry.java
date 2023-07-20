package org.uom.distributed.systems.registry;

import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.worker.Node;

import java.util.HashMap;
import java.util.concurrent.TransferQueue;

public class Registry {
    public static HashMap<String, Node> registry = new HashMap<>(10);
    public final static HashMap<String, TransferQueue<Message>> messageQueuesForClusters = new HashMap<>(10);

    public static void registerNode(Node node) {
        registry.put(node.getNodeName(), node);
    }

    public static Node query(String recipient) {
        return registry.get(recipient);
    }
}

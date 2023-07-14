package org.uom.distributed.systems.registry;

import org.uom.distributed.systems.worker.Node;

import java.util.HashMap;

public class Registry {
    public static HashMap<String, Node> registry = new HashMap<>(10);

    public static void registerNode(Node node) {
        registry.put(node.getNodeName(), node);
    }

    public static Node query(String recipient) {
        return registry.get(recipient);
    }
}

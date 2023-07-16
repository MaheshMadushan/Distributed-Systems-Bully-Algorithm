package org.uom.distributed.systems.messaging;

import org.uom.distributed.systems.registry.Registry;
import org.uom.distributed.systems.worker.Node;

import java.util.HashMap;

public class MessageService {
    private HashMap<String, Node> registry = null;

    public MessageService() {
        registry = Registry.registry; // caching
    }

    public void sendMessage(String recipient, Message message) throws InterruptedException {
        Node recipientNode;
        if (!registry.containsKey(recipient)) {
            recipientNode = Registry.query(recipient);
            registry.put(recipientNode.getNodeName(), recipientNode); // caching
        } else {
            recipientNode = registry.get(recipient);
        }
        recipientNode.receiveMessage(message);
    }
}

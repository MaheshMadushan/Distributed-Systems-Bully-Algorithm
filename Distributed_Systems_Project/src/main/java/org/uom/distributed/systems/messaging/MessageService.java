package org.uom.distributed.systems.messaging;

import org.uom.distributed.systems.registry.Registry;
import org.uom.distributed.systems.worker.Node;

import java.util.HashMap;
import java.util.concurrent.TransferQueue;

public class MessageService {
    private final HashMap<String, Node> registry;
    public final static HashMap<String, TransferQueue<Message>> messageQueuesForClusters = new HashMap<>(10);

    public MessageService() {
        registry = Registry.registry; // caching
    }

    public void sendMessage(Message message) {
        Node recipientNode = getRecipientNode(message.getRecipientOrGroupID());
        recipientNode.receiveMessage(message);
    }


    private Node getRecipientNode(String recipient) {
        Node recipientNode;
        if (!registry.containsKey(recipient)) {
            recipientNode = Registry.query(recipient); // querying is expensive
            registry.put(recipientNode.getNodeName(), recipientNode); // caching
        } else {
            recipientNode = registry.get(recipient);
        }
        return recipientNode;
    }
}

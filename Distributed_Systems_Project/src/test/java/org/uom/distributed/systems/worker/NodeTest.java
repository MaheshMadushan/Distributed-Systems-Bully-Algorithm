package org.uom.distributed.systems.worker;

import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.worker.middleware.FollowerMiddleware;
import org.uom.distributed.systems.worker.middleware.LeaderMiddleware;

class NodeTest {
    @org.junit.jupiter.api.Test
    void onMessageTest() throws InterruptedException {
        Node node = new Node(4,4,40);

        Thread thread = new Thread(node);
        thread.start();
        node.setMiddleware(new FollowerMiddleware(node));
        node.sendMessage(new Message(MessageType.OK, ""));
        node.sendMessage(new Message(MessageType.COORDINATOR, ""));
        node.sendMessage(new Message(MessageType.ELECTION, ""));
        node.sendMessage(new Message(MessageType.TASK, ""));

        node.setMiddleware(new LeaderMiddleware(node));
        node.sendMessage(new Message(MessageType.OK, ""));
        node.sendMessage(new Message(MessageType.COORDINATOR, ""));
        node.sendMessage(new Message(MessageType.ELECTION, ""));
        node.sendMessage(new Message(MessageType.TASK, ""));

        thread.join();
    }
}
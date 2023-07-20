package org.uom.distributed.systems.worker;

import org.uom.distributed.systems.worker.middleware.FollowerMiddleware;
import org.uom.distributed.systems.worker.middleware.LeaderMiddleware;

class NodeTest {
    @org.junit.jupiter.api.Test
    void onMessageTest() throws InterruptedException {
        Node node = new Node(4,4,40);

        Thread thread = new Thread(node);
        node.setMiddleware(new FollowerMiddleware(node));
        thread.start();
        node.startNewMiddlewareProcess();

        node.stopRunningMiddlewareProcessGracefully();
        node.setMiddleware(new LeaderMiddleware(node));
        node.startNewMiddlewareProcess();
//        node.restartMiddlewareThread();
//        node.receiveMessage(new Message(MessageType.OK, recipient));
//        node.receiveMessage(new Message(MessageType.COORDINATOR, recipient));
//        node.receiveMessage(new Message(MessageType.ELECTION, recipient));
//        node.receiveMessage(new Message(MessageType.TASK, recipient));
//
//        node.setMiddleware(new LeaderMiddleware(node));
//        node.receiveMessage(new Message(MessageType.OK, recipient));
//        node.receiveMessage(new Message(MessageType.COORDINATOR, recipient));
//        node.receiveMessage(new Message(MessageType.ELECTION, recipient));
//        node.receiveMessage(new Message(MessageType.TASK, recipient));

        thread.join();
    }
}
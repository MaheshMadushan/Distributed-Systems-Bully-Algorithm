package org.uom.distributed.systems.worker;

import org.uom.distributed.systems.NodeManager;

class NodeTest {
    @org.junit.jupiter.api.Test
    void onMessageTest() throws InterruptedException {
        Node[] nodes = {
                new Node(4,1,4000),
                new Node(4,2,4000),
                new Node(4,3,4000),
                new Node(3,4,4000),
                new Node(2,4,4000),
                new Node(1,4,4000)
        };

        NodeManager.initiateSystem(nodes);


//        Thread thread = new Thread(node);
//        node.setMiddleware(new FollowerMiddleware(node));
//        thread.start();
//        node.startNewMiddlewareProcess();
//
//        node.stopRunningMiddlewareProcessGracefully();
//        node.setMiddleware(new LeaderMiddleware(node));
//        node.startNewMiddlewareProcess();
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




        Thread.sleep(4000);
        nodes[5].kill();
        Thread.sleep(150000);
        nodes[4].kill();
        Thread.sleep(9000);
        nodes[3].kill();
        Thread.sleep(Integer.MAX_VALUE);

    }
}
package org.uom.distributed.systems.worker.middleware;

import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.HashMap;

public class IdleState implements IMiddleware {
    private Node host;

    public IdleState(Node host) {
        this.host = host;
    }
    @Override
    public MiddlewareType getMiddlewareType() {
        return MiddlewareType.IDLE;
    }

    @Override
    public void handle(Message message) {
        if (message.getType().name().equals("ASSIGN")) {
            HashMap<String, String> fields = message.getFields();
            if (fields.get("TYPE").equals("LEADER")) {
                LeaderMiddleware leaderMiddleware = new LeaderMiddleware(host);
                leaderMiddleware.setGroupID(fields.get("GROUP_ID"));
                host.setMiddleware(leaderMiddleware);
                host.startNewMiddlewareProcess();
                System.out.println(host.getNodeName() + " with bully id " + host.getNodeBullyID() + " " + "Assigned as Leader.");
            } else if (fields.get("TYPE").equals("FOLLOWER")) {
                FollowerMiddleware followerMiddleware = new FollowerMiddleware(host);
                followerMiddleware.setGroupID(fields.get("GROUP_ID"));
                followerMiddleware.setLeader(fields.get("LEADER"));
                host.setMiddleware(followerMiddleware);
                host.startNewMiddlewareProcess();
                System.out.println(host.getNodeName() + " with bully id " + host.getNodeBullyID() + "Assigned as Follower of leader" + " " + fields.get("LEADER"));
            }
        }
    }

    @Override
    public void receiveMessage(Message message) {
        this.handle(message);
    }

    @Override
    public void sendMessage(String recipientAddress, Message message) {

    }

    @Override
    public void stopProcess() {

    }

    @Override
    public void startProcess() {

    }

}

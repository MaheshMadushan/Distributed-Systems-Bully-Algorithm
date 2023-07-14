package org.uom.distributed.systems.worker.middleware;

import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IdleState implements IMiddleware {
    private Node node;

    public IdleState(Node node) {
        this.node = node;
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
                LeaderMiddleware leaderMiddleware = new LeaderMiddleware(node);
                leaderMiddleware.setGroupID(fields.get("GROUP_ID"));
                node.setMiddleware(leaderMiddleware);
                System.out.println("Assigned as Leader.");
            } else if (fields.get("TYPE").equals("FOLLOWER")) {
                FollowerMiddleware followerMiddleware = new FollowerMiddleware(node);
                followerMiddleware.setGroupID(fields.get("GROUP_ID"));
                followerMiddleware.setLeader(fields.get("LEADER"));
                node.setMiddleware(followerMiddleware);
                System.out.println("Assigned as Follower.");
            }
        }
    }

    @Override
    public void run() {
        // do idle specific sanitation tasks
        // separate thread runs
    }
}

package org.uom.distributed.systems.worker.middleware;

import org.json.JSONObject;
import org.uom.distributed.systems.LogInterceptor;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.HashMap;

public class IdleState implements IMiddleware {
    private Node host;
    private final LogInterceptor LOGGER ;

    public IdleState(Node host) {
        this.host = host;
        this.LOGGER = new LogInterceptor(FollowerMiddleware.class, host.getWSLogConnection());
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
                JSONObject response = new JSONObject()
                        .put("MESSAGE_TYPE", "INITIAL_GROUPING")
                        .put("ASSIGNED_AS", "LEADER")
                        .put("NODE_NAME", host.getNodeName())
                        .put("GROUP_ID", fields.get("GROUP_ID"));
                this.host.getWSCommonConnection().send(response.toString());
                LOGGER.info(host.getNodeName() + " with bully id " + host.getNodeBullyID() + " " + "Assigned as Leader.");
            } else if (fields.get("TYPE").equals("FOLLOWER")) {
                FollowerMiddleware followerMiddleware = new FollowerMiddleware(host);
                followerMiddleware.setGroupID(fields.get("GROUP_ID"));
                followerMiddleware.setLeader(fields.get("LEADER"));
                host.setMiddleware(followerMiddleware);
                host.startNewMiddlewareProcess();
                JSONObject response = new JSONObject()
                        .put("MESSAGE_TYPE", "INITIAL_GROUPING")
                        .put("ASSIGNED_AS", "FOLLOWER")
                        .put("LEADER", fields.get("LEADER"))
                        .put("NODE_NAME", host.getNodeName())
                        .put("GROUP_ID", fields.get("GROUP_ID"));
                this.host.getWSCommonConnection().send(response.toString());
                LOGGER.info(host.getNodeName() + " with bully id " + host.getNodeBullyID() + "Assigned as Follower of leader" + " " + fields.get("LEADER"));
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

    @Override
    public String getGroupID() {
        return null;
    }

}

package org.uom.distributed.systems.worker.middleware;

import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageService;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LeaderMiddleware implements IMiddleware {
    private String groupID;
    private final List<String> followers;
    private final List<Node> boardOfExecutives;
    private Node node;
    private final MessageService messageService = new MessageService();

    public LeaderMiddleware(Node node) {
        this.node = node;
        this.followers = new ArrayList<>(10);
        this.boardOfExecutives = new ArrayList<>(10);
    }

    public LeaderMiddleware(Node node , List<Node> boardOfExecutives ) {
        this.node = node;
        this.followers = new ArrayList<>(10);
        this.boardOfExecutives = boardOfExecutives;
    }

    public void setGroupID (String groupID) {
        this.groupID = groupID;
    }

    public String getGroupID() {
        return groupID;
    }

    public void addFellowLeader(Node leadingState) {
        boardOfExecutives.add(leadingState);
    }

    public void addFollower(String follower) {
        followers.add(follower);
    }

    @Override
    public MiddlewareType getMiddlewareType() {
        return MiddlewareType.LEADER;
    }

    @Override
    public void handle(Message message) {
        HashMap<String, String> fields = message.getFields();
        switch (message.getType().name()) {
            case "ASSIGN" :
                if(fields.get("TYPE").equals("FOLLOWER")) {
                    // graceful termination of leader processes should handle
                    FollowerMiddleware followerMiddleware = new FollowerMiddleware(node);
                    followerMiddleware.setGroupID(fields.get("GROUP_ID"));
                    followerMiddleware.setLeader(fields.get("LEADER"));
                    node.setMiddleware(followerMiddleware);
                    System.out.println("Assigned as Follower.");
                }
                else {
                    System.out.println("message is discarded");
                }
                break;
            case "TASK" :
                System.out.println("Task received for Leader.");
                break;
            case "ELECTION" :
                System.out.println("Election received for Leader.");
                break;
            case "OK" :
                System.out.println("OK received for Leader.");
                break;
            case "COORDINATOR" :
                System.out.println("Coordinator received for Leader.");
                break;
            case "ADD_FOLLOWER" :
                this.addFollower(fields.get("FOLLOWER_NAME"));
                System.out.println("Follower added to the leader.");
                break;
        }
    }

    @Override
    public void run() {
        // do leader specific sanitation tasks
        // separate thread runs
    }
}

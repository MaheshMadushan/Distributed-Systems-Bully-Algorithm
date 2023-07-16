package org.uom.distributed.systems.worker.middleware;

import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FollowerMiddleware implements IMiddleware {
    private final Node host;
    private String groupID;
    private String leader;
    private final List<String> groupMembers;
    private final BlockingQueue<Message> messageBlockingQueue
            = new ArrayBlockingQueue<>(10);

    public FollowerMiddleware(Node host) {
        this.host = host;
        this.groupMembers = new ArrayList<>(10);
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public void setGroupID (String groupID) {
        this.groupID = groupID;
    }

    public String getGroupID() {
        return groupID;
    }

    public void addGroupMembers (String groupMember) {
        groupMembers.add(groupMember);
    }

    @Override
    public MiddlewareType getMiddlewareType() {
        return MiddlewareType.FOLLOWER;
    }

    @Override
    public void handle(Message message) {
        HashMap<String, String> fields = message.getFields();
        switch (message.getType().name()) {
            case "ASSIGN" :
                if(fields.get("TYPE").equals("LEADER")) {
                    // graceful termination of follower processes
                    LeaderMiddleware leaderMiddleware = new LeaderMiddleware(host);
                    leaderMiddleware.setGroupID(fields.get("GROUP_ID"));
                    host.setMiddleware(leaderMiddleware);
                    System.out.println("Assigned as Leader.");
                }
                else {
                    System.out.println("message is discarded");
                }
                break;
            case "TASK" :
                System.out.println("Task received for follower.");
                break;
            case "ELECTION" :
                System.out.println("Election received for follower.");
                break;
            case "OK" :
                System.out.println("OK received for follower.");
                break;
            case "COORDINATOR" :
                System.out.println("Coordinator received for follower.");
                break;
            default :
                fields.forEach((s, s2) -> System.out.println(s+":"+s2));

        }
    }

    @Override
    public void run() {
        // do follower specific sanitation tasks
        // separate thread runs
        // send statistics to the leader
        // graceful termination of follower on elected as leader
        int i = 0;
//        while () {
//
//        }
    }
}

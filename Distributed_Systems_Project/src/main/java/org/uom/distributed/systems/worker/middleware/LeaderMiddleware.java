package org.uom.distributed.systems.worker.middleware;

import org.uom.distributed.systems.Config;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageService;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

public class LeaderMiddleware implements IMiddleware {
    private String groupID;
    private final List<String> followers;
    private final List<Node> boardOfExecutives;
    private final Node host;

    private final BlockingQueue<Message> messageSendingBlockingQueue = new ArrayBlockingQueue<>(10);
    private final BlockingQueue<Message> messageReceivingBlockingQueue = new ArrayBlockingQueue<>(10);
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public LeaderMiddleware(Node host) {
        this.host = host;
        this.followers = new ArrayList<>(10);
        this.boardOfExecutives = new ArrayList<>(10);
    }

    public LeaderMiddleware(Node host , List<Node> boardOfExecutives ) {
        this.host = host;
        this.followers = new ArrayList<>(10);
        this.boardOfExecutives = boardOfExecutives;
    }

    public void setGroupID (String groupID) {
        this.groupID = groupID;
        MessageService.messageQueuesForClusters.put(groupID, new LinkedTransferQueue<>());
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
                    FollowerMiddleware followerMiddleware = new FollowerMiddleware(host);
                    followerMiddleware.setGroupID(fields.get("GROUP_ID"));
                    followerMiddleware.setLeader(fields.get("LEADER"));
                    host.setMiddleware(followerMiddleware);
                    System.out.println(host.getNodeName() + " " + "Assigned as Follower.");
                }
                else {
                    System.out.println("message is discarded");
                }
                break;
            case "TASK" :
                System.out.println(host.getNodeName() + " " + "Task received for Leader.");
                break;
            case "ELECTION" :
                System.out.println(host.getNodeName() + " " + "Election received for Leader.");
                break;
            case "OK" :
                System.out.println(host.getNodeName() + " " + "OK received for Leader.");
                break;
            case "COORDINATOR" :
                System.out.println(host.getNodeName() + " " + "Coordinator received for Leader.");
                break;
            case "ADD_FOLLOWER" :
                String newFollowerName = fields.get("FOLLOWER_NAME");
                this.addFollower(newFollowerName);
                for (String follower : followers) {
                    if (!newFollowerName.equals(follower)) {
                        Message addGroupMemberMessage = new Message(MessageType.ADD_GROUP_MEMBER, follower);
                        addGroupMemberMessage.addField("GROUP_MEMBER_NAME", newFollowerName);
                        addGroupMemberMessage.addField("GROUP_MEMBER_BULLY_ID",fields.get("FOLLOWER_BULLY_ID"));
                        try {
                            host.sendMessage(addGroupMemberMessage);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                System.out.println(host.getNodeName() + " " + "Follower added to the leader.");
                break;
            default :
                fields.forEach((s, s2) -> System.out.println(s+":"+s2));
        }
    }

    @Override
    public void receiveMessage(Message message) {
        messageReceivingBlockingQueue.add(message);
    }


    @Override
    public void sendMessage(String recipientAddress, Message message) {
        messageSendingBlockingQueue.add(message);
    }

    @Override
    public void stopProcess() {
        messageReceivingBlockingQueue.add(new Message(MessageType.INTERRUPT, null));
        messageSendingBlockingQueue.add(new Message(MessageType.INTERRUPT, null));
        executorService.shutdownNow();
    }

    @Override
    public void startProcess() {
        Thread messageReceivingThread = new Thread(() -> {
            try {
                while (true) {
                    Message message = messageReceivingBlockingQueue.take();
                    if (message.getType().equals(MessageType.INTERRUPT)) {
                        break;
                    }
                    this.handle(message);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        Thread messageSendingThread = new Thread(() -> {
            try {
                while (true) {
                    Message message = messageSendingBlockingQueue.take();
                    if (message.getType().equals(MessageType.INTERRUPT)) {
                        break;
                    }
                    host.sendMessage(message);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        messageReceivingThread.start();
        messageSendingThread.start();

        executorService.scheduleWithFixedDelay(() -> {
            for (String follower : followers) {
                Message beaconMessage = new Message(MessageType.BEACON, follower);
                try {
                    host.sendMessage(beaconMessage);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, Config.UNIT_TIME, Config.UNIT_TIME * 10, TimeUnit.MILLISECONDS);
    }
}

package org.uom.distributed.systems.worker.middleware;

import org.json.JSONObject;
import org.uom.distributed.systems.Config;
import org.uom.distributed.systems.LogInterceptor;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class LeaderMiddleware implements IMiddleware {
    private final LogInterceptor LOGGER ;
    private String groupID;
    private final HashMap<Integer, String> followers;
    private final List<Node> boardOfExecutives;
    private final Node host;
    private final BlockingQueue<Message> messageSendingBlockingQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> messageReceivingBlockingQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean processIsActive = new AtomicBoolean(false);

    public LeaderMiddleware(Node host) {
        this.host = host;
        this.followers = new HashMap<>(10);
        this.boardOfExecutives = new ArrayList<>(10);
        this.LOGGER = new LogInterceptor(LeaderMiddleware.class, host.getWSLogConnection());
    }
    public void setGroupID (String groupID) {
        this.groupID = groupID;

//        JSONObject response = new JSONObject()
//                .put("NODE_NAME", host.getNodeName())
//                .put("ASSIGNED_AS", MiddlewareType.LEADER)
//                .put("GROUP_ID", groupID);
//        host.getWebSocketServer().send();(response.toString());
    }
    public String getGroupID() {
        return groupID;
    }

    public void addFellowLeader(Node leadingState) {
        boardOfExecutives.add(leadingState);
    }

    public void addFollower(int bullyID, String follower) {
        followers.put(bullyID, follower);
//        JSONObject response = new JSONObject()
//                .put("NODE_NAME", host.getNodeName())
//                .put("ACTING_AS", MiddlewareType.LEADER)
//                .put("ADD_FOLLOWER", follower)
//                .put("BULLY_ID", bullyID);
//        host.getWebSocketServer().send();(response.toString());
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
                    LOGGER.info(host.getNodeName() + " " + "Assigned as Follower.");
                }
                else {
                    LOGGER.info("message is discarded");
                }
                break;
            case "TASK" :
                LOGGER.info(host.getNodeName() + " " + "Task received for Leader.");
                break;
            case "ELECTION" :
                LOGGER.info("from " + fields.get("SENDER") + " ELECTION message received for  leader " + host.getNodeName());

                int electionInitiatorBullyID = Integer.parseInt(fields.get("CANDIDATE_BULLY_ID"));
                if (electionInitiatorBullyID < host.getNodeBullyID()) {
                    message = new Message(MessageType.COORDINATOR, fields.get("SENDER"));
                    host.sendMessage(message.addField("NEW_LEADER", this.host.getNodeName()));
                }
                LOGGER.info("Leader " + host.getNodeName() + " received Election message.");
                break;
            case "OK" :
                LOGGER.info(host.getNodeName() + " " + "OK received for Leader.");
                break;
            case "COORDINATOR" :
                LOGGER.info(host.getNodeName() + " " + "Coordinator received for Leader.");
                break;
            case "ADD_FOLLOWER" :
                String newFollowerName = fields.get("FOLLOWER_NAME");
                int followerBullyID = Integer.parseInt(fields.get("FOLLOWER_BULLY_ID"));
                this.addFollower(followerBullyID, newFollowerName);
                for (Map.Entry<Integer, String> follower : followers.entrySet()) {
                    if (!newFollowerName.equals(follower.getValue())) {
                        Message addGroupMemberMessage = new Message(MessageType.ADD_GROUP_MEMBER, follower.getValue());
                        addGroupMemberMessage.addField("GROUP_MEMBER_NAME", newFollowerName);
                        addGroupMemberMessage.addField("GROUP_MEMBER_BULLY_ID", String.valueOf(followerBullyID));
                        host.sendMessage(addGroupMemberMessage);
                    }
                }
                LOGGER.info("Node " + newFollowerName + " Follower added to the leader " + host.getNodeName());
                break;
            default :
                fields.forEach((s, s2) -> LOGGER.info(s+":"+s2));
        }
    }

    @Override
    public void receiveMessage(Message message) {
        if(processIsActive.get()) messageReceivingBlockingQueue.add(message);
    }

    @Override
    public void sendMessage(String recipientAddress, Message message) {
        if(processIsActive.get()) messageSendingBlockingQueue.add(message);
    }

    @Override
    public synchronized void stopProcess() {
        if (processIsActive.get()) {
            messageReceivingBlockingQueue.add(new Message(MessageType.INTERRUPT, null));
            messageSendingBlockingQueue.add(new Message(MessageType.INTERRUPT, null));
            executorService.shutdownNow();
            processIsActive.set(false);
            LOGGER.info(host.getNodeName() + " leader process shutting down");
            JSONObject response = new JSONObject()
                    .put("NODE_NAME", host.getNodeName())
                    .put("STOPPED_LEADER_PROCESS", true);
            host.getWSCommonConnection().send(response.toString());
        }
    }

    @Override
    public synchronized void startProcess() {
        LOGGER.info(host.getNodeName() + " leader process starting");
        if (!processIsActive.get())
            processIsActive.set(true);
        else
            return;

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
        }, this.host.getNodeName() + "-Leader-messageReceivingThread");
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
        }, this.host.getNodeName() + "-Leader-messageSendingThread");

        messageReceivingThread.start();
        messageSendingThread.start();

        executorService.scheduleWithFixedDelay(() -> {
            LOGGER.info(host.getNodeName() + " leader process beacon activated");
            LOGGER.info(host.getNodeName() + " followers " + followers);
            for (Map.Entry<Integer, String> follower : followers.entrySet()) {
                Message beaconMessage = new Message(MessageType.BEACON, follower.getValue());
                host.sendMessage(beaconMessage);
            }
        }, 0, Config.UNIT_TIME * 10, TimeUnit.MILLISECONDS);

        JSONObject response = new JSONObject()
                .put("MESSAGE_TYPE", "POST_ELECTION_LEADER")
                .put("FOLLOWERS", followers)
                .put("NODE_NAME", host.getNodeName());
        this.host.getWSCommonConnection().send(response.toString());
    }
}

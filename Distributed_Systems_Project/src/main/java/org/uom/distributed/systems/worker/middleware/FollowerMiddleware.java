package org.uom.distributed.systems.worker.middleware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uom.distributed.systems.Utilities.Timer;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FollowerMiddleware implements IMiddleware {
    public static Logger LOGGER = LoggerFactory.getLogger(FollowerMiddleware.class);

    private final Node host;
    private String groupID;
    private String leader;
    private final HashMap<Integer, String> groupMembers;
    private final BlockingQueue<Message> messageReceivingBlockingQueue
            = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> messageSendingBlockingQueue
            = new LinkedBlockingQueue<>();
    private final Timer timer = new Timer(15);
    private final AtomicBoolean timerThreadInterrupted = new AtomicBoolean(false);
    private final AtomicBoolean processIsActive = new AtomicBoolean(false);
    private final Object lock = new Object();
    private final BlockingQueue<Message> electionDecisionCommunicationBlockinqQueue =
            new LinkedBlockingQueue<>();

    public FollowerMiddleware(Node host) {
        this.host = host;
        this.groupMembers = new HashMap<>(10);
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

    public void addGroupMembers (int bullyId, String groupMember) {
        groupMembers.put(bullyId, groupMember);
    }

    @Override
    public MiddlewareType getMiddlewareType() {
        return MiddlewareType.FOLLOWER;
    }
    public synchronized void stopProcess() {
        messageReceivingBlockingQueue.add(new Message(MessageType.INTERRUPT, null)); // Poison pill strategy
        messageSendingBlockingQueue.add(new Message(MessageType.INTERRUPT, null)); // Poison pill strategy
        processIsActive.set(false);
        LOGGER.info(host.getNodeName() + " follower process shutting down");
    }

    @Override
    public synchronized void startProcess() {
        processIsActive.set(true);
        Thread leaderPingingThread = new Thread(() -> {
            while (true) {
                sendMessage(leader, new Message(MessageType.PING, leader));
            }
        });

    }

    @Override
    public Message handle(Message message) throws InterruptedException {
        HashMap<String, String> fields = message.getFields();
        switch (message.getType().name()) {
            case "ASSIGN" :
                if(fields.get("TYPE").equals("LEADER")) {
                    // graceful termination of follower processes
                    LeaderMiddleware leaderMiddleware = new LeaderMiddleware(host);
                    leaderMiddleware.setGroupID(fields.get("GROUP_ID"));
//                    if(fields.containsKey("FOLLOWERS")) {
//                        String[] followers = fields.get("FOLLOWERS").split(",");
//                        for (String follower : followers) {
//                            leaderMiddleware.addFollower(follower);
//                        }
//                    }
                    host.stopRunningMiddlewareProcessGracefully();
                    host.setMiddleware(leaderMiddleware);
                    host.startNewMiddlewareProcess();
                    LOGGER.info(host.getNodeName() + " Assigned as Leader");
                }
                break;
            case "ELECTION" :
                // send to election message handling thread
                break;
            case "OK" :
                // send to ok message handling thread
                break;
            case "COORDINATOR" :
                // send to coordinator message handling thread
                break;
            case "ADD_GROUP_MEMBER" :
                String newGroupMemberAddress = fields.get("GROUP_MEMBER_NAME");
                int newGroupMemberBullyID = Integer.parseInt(fields.get("GROUP_MEMBER_BULLY_ID"));
                this.addGroupMembers(newGroupMemberBullyID, newGroupMemberAddress);
                // bro, add me to your group
                if(fields.containsKey("MY_GROUP_MEMBERS")) {
                    String[] a = fields.get("MY_GROUP_MEMBERS").split(",");
                    List<String> sendersGroupMembers = List.of(fields.get("MY_GROUP_MEMBERS").split(","));
                    if (!sendersGroupMembers.contains(String.valueOf(host.getNodeBullyID()))){
                        Message addMeToNewGroupMemberMessage = new Message(MessageType.ADD_GROUP_MEMBER, newGroupMemberAddress);
                        addMeToNewGroupMemberMessage.addField("GROUP_MEMBER_NAME", host.getNodeName());
                        addMeToNewGroupMemberMessage.addField("GROUP_MEMBER_BULLY_ID",String.valueOf(host.getNodeBullyID()));
                        StringBuilder groupMembersAsString = new StringBuilder();
                        groupMembers.keySet().stream().iterator().forEachRemaining((i) -> {
                            groupMembersAsString.append(i);
                            groupMembersAsString.append(",");
                        });
                        addMeToNewGroupMemberMessage.addField("MY_GROUP_MEMBERS", groupMembersAsString.toString());

                        host.sendMessage(addMeToNewGroupMemberMessage);
                    }
                } else {
                    Message addMeToNewGroupMemberMessage = new Message(MessageType.ADD_GROUP_MEMBER, newGroupMemberAddress);
                    addMeToNewGroupMemberMessage.addField("GROUP_MEMBER_NAME", host.getNodeName());
                    addMeToNewGroupMemberMessage.addField("GROUP_MEMBER_BULLY_ID",String.valueOf(host.getNodeBullyID()));
                    StringBuilder groupMembersAsString = new StringBuilder();
                    groupMembers.keySet().stream().iterator().forEachRemaining((i) -> {
                        groupMembersAsString.append(i);
                        groupMembersAsString.append(",");
                    });
                    addMeToNewGroupMemberMessage.addField("MY_GROUP_MEMBERS", groupMembersAsString.toString());

                    host.sendMessage(addMeToNewGroupMemberMessage);
                }

                LOGGER.info("adding member " + newGroupMemberAddress + " with bully id " + newGroupMemberBullyID + " to node " + host.getNodeName() + " as group member.");
                break;
            default :
                fields.forEach((s, s2) -> LOGGER.info(s+":"+s2));

        }
    }

    @Override
    public Message receiveMessage(Message message) {
        if (processIsActive.get())
            return handle(message);
        return null;
    }


    @Override
    public void sendMessage(String recipientAddress, Message message) {
        if (processIsActive.get())
            messageSendingBlockingQueue.add(message);
    }

}

package org.uom.distributed.systems.worker.middleware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uom.distributed.systems.Config;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageService;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class LeaderMiddleware implements IMiddleware {
    public static Logger LOGGER = LoggerFactory.getLogger(LeaderMiddleware.class);
    private String groupID;
    private final Map<Integer, String> followers;
    private final List<Node> boardOfExecutives;
    private final Node host;
    private final AtomicBoolean processIsActive = new AtomicBoolean(false);

    public LeaderMiddleware(Node host) {
        this.host = host;
        this.followers = new HashMap<>(10);
        this.boardOfExecutives = new ArrayList<>(10);
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

    public void addFollower(int bullyID, String follower) {
        followers.add(bullyID, follower);
    }

    @Override
    public MiddlewareType getMiddlewareType() {
        return MiddlewareType.LEADER;
    }

    @Override
    public Message handle(Message message) {
        HashMap<String, String> fields = message.getFields();
        Message reply;
        switch (message.getType().name()) {
            case "ASSIGN" :
                if(fields.get("TYPE").equals("FOLLOWER")) {
                    // graceful termination of leader processes should handle
                    FollowerMiddleware followerMiddleware = new FollowerMiddleware(host);
                    followerMiddleware.setGroupID(fields.get("GROUP_ID"));
                    followerMiddleware.setLeader(fields.get("LEADER"));
                    host.setMiddleware(followerMiddleware);
                    LOGGER.info(host.getNodeName() + " " + "Assigned as Follower.");
                    reply = new Message(MessageType.SUCCESS, fields.get("SENDER"));
                }
                else {
                    LOGGER.info("message is discarded");
                }
                break;
            case "ELECTION" :
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
                int bullyIDOfFollower = Integer.parseInt(fields.get("FOLLOWER_BULLY_ID"));
                this.addFollower(bullyIDOfFollower, newFollowerName);
                for (Map.Entry<Integer, String> follower : followers.entrySet()) {
                    if (!newFollowerName.equals(follower.getValue())) {
                        Message addGroupMemberMessage = new Message(MessageType.ADD_GROUP_MEMBER, follower.getValue());
                        addGroupMemberMessage.addField("GROUP_MEMBER_NAME", newFollowerName);
                        addGroupMemberMessage.addField("GROUP_MEMBER_BULLY_ID",String.valueOf(bullyIDOfFollower));
                        host.sendMessage(addGroupMemberMessage);
                    }
                }
                LOGGER.info("Node " + newFollowerName + " Follower added to the leader " + host.getNodeName());
                break;
            default :
                fields.forEach((s, s2) -> LOGGER.info(s+":"+s2));
        }
        return reply;
    }

    @Override
    public Message receiveMessage(Message message) {
        if(processIsActive.get()) return handle(message);
        return null;
    }

    @Override
    public Message sendMessage(String recipientAddress, Message message) {
        if(processIsActive.get()) host.sendMessage(message);
        return null;
    }

    @Override
    public synchronized void stopProcess() {
        if (processIsActive.get()) {
            processIsActive.set(false);
            LOGGER.info(host.getNodeName() + " leader process shutting down");
        }
    }

    @Override
    public synchronized void startProcess() {
        LOGGER.info(host.getNodeName() + " leader process starting");
        if (!processIsActive.get())
            processIsActive.set(true);
        else
            return;

    }
}

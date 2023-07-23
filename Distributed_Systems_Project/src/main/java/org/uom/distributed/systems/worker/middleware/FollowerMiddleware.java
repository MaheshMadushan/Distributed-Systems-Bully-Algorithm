package org.uom.distributed.systems.worker.middleware;

import org.json.JSONObject;
import org.uom.distributed.systems.Utilities.LogInterceptor;
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
    private final LogInterceptor LOGGER ;

    private final Node host;
    private String groupID;
    private String leader;
    private final HashMap<Integer, String> groupMembers;
    private final BlockingQueue<Message> messageReceivingBlockingQueue
            = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> messageSendingBlockingQueue
            = new LinkedBlockingQueue<>();
    private final AtomicInteger counter = new AtomicInteger(15);
    private final Timer timer = new Timer(15);
    private final AtomicBoolean timerThreadInterrupted = new AtomicBoolean(false);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean isCandidate = new AtomicBoolean(false);
    private final AtomicBoolean processIsActive = new AtomicBoolean(false);
    private final Thread timerThread;
    private final Object lock = new Object();
    private final BlockingQueue<Message> electionDecisionCommunicationBlockinqQueue =
            new LinkedBlockingQueue<>();

    public FollowerMiddleware(Node host) {
        this.host = host;
        this.groupMembers = new HashMap<>(10);
        this.LOGGER = new LogInterceptor(FollowerMiddleware.class, host.getWSLogConnection());
        this.timerThread = new Thread(() -> {
            while (!timerThreadInterrupted.get()) {
                timer.start();
                synchronized (lock) {
                    LOGGER.info(host.getStateType() + " node " + host.getNodeName() + " received beacon " + 15 + " seconds ago."
                                    + " assuming leader is unresponsive."
                    );

                    JSONObject response = new JSONObject();

                    List<Map.Entry<Integer, String>> bullies = groupMembers
                            .entrySet()
                            .stream()
                            .filter(
                                    groupMemberIDAndNodeAddress -> groupMemberIDAndNodeAddress.getKey() > this.host.getNodeBullyID()
                            )
                            .collect(Collectors.toList());

                    Message message;

                    if (bullies.isEmpty()) {
                        // im the leader since no bully id greater than mine is presents
                        isCandidate.set(true);
                        electionInProgress.set(true);

                        response = new JSONObject()
                                .put("MESSAGE_TYPE", "ELECTION")
                                .put("BEACON_TIMER_EXECUTED", true)
                                .put("NODE_NAME", host.getNodeName());
                        host.getWSCommonConnection().send(response.toString());

                        for (Map.Entry<Integer, String> innocentIDAndNodeAddress : groupMembers.entrySet()) {
                            message = new Message(MessageType.OK, innocentIDAndNodeAddress.getValue());
                            host.sendMessage(message);

                            message = new Message(MessageType.COORDINATOR, innocentIDAndNodeAddress.getValue());
                            message.addField("NEW_LEADER", this.host.getNodeName());
                            host.sendMessage(message);
                        }

                        StringBuilder followers = new StringBuilder();
                        groupMembers.entrySet().stream().iterator().forEachRemaining((i) -> {
                            followers.append(i.getKey())
                                    .append("=")
                                    .append(i.getValue())
                                    .append(",");
                        });

                        Message assignMessage = new Message(MessageType.ASSIGN, this.host.getNodeName())
                                .addField("TYPE", MiddlewareType.LEADER.toString())
                                .addField("GROUP_ID", groupID)
                                .addField("FOLLOWERS", followers.toString());
                        host.sendMessage(assignMessage);

                        isCandidate.set(false);
                        electionInProgress.set(false);

                        timerThreadInterrupted.set(true);
                        processIsActive.set(false); // deactivate follower process

                        break;
                    } else {
                        if(!isCandidate.get() && !electionInProgress.get()) {

                            LOGGER.info(host.getNodeName() + " in a election");
                            electionInProgress.set(true);
                            isCandidate.set(true);

                            response = new JSONObject()
                                    .put("MESSAGE_TYPE", "ELECTION")
                                    .put("BEACON_TIMER_EXECUTED", true)
                                    .put("NODE_NAME", host.getNodeName())
                                    .put("IN_ELECTION", electionInProgress.get())
                                    .put("IS_CANDIDATE", isCandidate.get())
                                    .put("NO_BULLIES", false);
                            try {
                                Thread.sleep(1500);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            host.getWSCommonConnection().send(response.toString());

                            // there are bully ids greater than mine and start the election
                            for (Map.Entry<Integer, String> bullyIDAndAddress : bullies) {
                                message = new Message(MessageType.ELECTION, bullyIDAndAddress.getValue());
                                message.addField("CANDIDATE_BULLY_ID", String.valueOf(host.getNodeBullyID()));

                                host.sendMessage(message);

                                response = new JSONObject()
                                        .put("MESSAGE_TYPE", "ELECTION")
                                        .put("NODE_NAME", host.getNodeName())
                                        .put("IN_ELECTION", electionInProgress.get())
                                        .put("IS_CANDIDATE", isCandidate.get())
                                        .put("SEND_CANDIDATE_TO", bullyIDAndAddress.getValue());
                                try {
                                    Thread.sleep(1500);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                host.getWSCommonConnection().send(response.toString());
                            }

                            electionDecisionCommunicationBlockinqQueue.clear(); // clear comm channel for the new thread process
                            Thread electionHandlingThread = new Thread(this::handleElection, this.host.getNodeName() + "-TimerThread-ElectionHandlingThread");
                            electionHandlingThread.start();

                        }
                    }
                }
                while (electionInProgress.get()) {

                }
            }
        }, this.host.getNodeName() + "-TimerThread");

    }

    private void handleElection() {

        // let 5 seconds for bully to answer via an OK or COORDINATOR
        // if not self promote to leader
        LOGGER.info("Election handling thread started for node " + host.getNodeName());

        JSONObject response = new JSONObject()
                .put("MESSAGE_TYPE", "ELECTION")
                .put("NODE_NAME", host.getNodeName())
                .put("IN_ELECTION", electionInProgress.get())
                .put("IS_CANDIDATE", isCandidate.get());
        host.getWSCommonConnection().send(response.toString());

        Message message;
        try {
            message = electionDecisionCommunicationBlockinqQueue.poll(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (isCandidate.get() && electionInProgress.get() && message == null) {
            response = new JSONObject()
                    .put("ASSIGNED_AS_LEADER", true)
                    .put("TIMEOUT", true);

            LOGGER.info("Elected this node " + host.getNodeName() + " since still no OK or COORDINATOR message received");

            StringBuilder followers = new StringBuilder();
            groupMembers.entrySet().stream().iterator().forEachRemaining((i) -> {
                followers.append(i.getKey())
                        .append("=")
                        .append(i.getValue())
                        .append(",");
            });

            Message assignMessage = new Message(MessageType.ASSIGN, this.host.getNodeName())
                    .addField("TYPE", MiddlewareType.LEADER.toString())
                    .addField("GROUP_ID", groupID)
                    .addField("FOLLOWERS", followers.toString());
            host.sendMessage(assignMessage);

            for (Map.Entry<Integer, String> innocentIDAndNodeAddress : groupMembers.entrySet()) {
                Message coordMessage = new Message(MessageType.COORDINATOR, innocentIDAndNodeAddress.getValue());
                coordMessage.addField("NEW_LEADER", this.host.getNodeName());
                host.sendMessage(coordMessage);
            }
        } else {
            if (message != null) {
                response = new JSONObject()
                        .put("ASSIGNED_AS_LEADER", false)
                        .put("TIMEOUT", false);

                LOGGER.info(host.getNodeName() + " " + message + (isCandidate.get() ? " and still a candidate" : "") + (electionInProgress.get() ? " and still election is going on." : ""));
                LOGGER.info("Not Elected this node " + host.getNodeName() + " since " + message.getFields().get("MESSAGE_TYPE") + " received");
            } else {
                response = new JSONObject()
                        .put("ASSIGNED_AS_LEADER", false)
                        .put("TIMEOUT", true);

                LOGGER.info(host.getNodeName() + " <- Not Elected this node since timeout and election is over or/and this node not a candidate ");
            }
        }
        electionInProgress.set(false);
        isCandidate.set(false);

//        response
//                .put("MESSAGE_TYPE", "ELECTION")
//                .put("NODE_NAME", host.getNodeName())
//                .put("IN_ELECTION", false)
//                .put("IS_CANDIDATE", false);
//        host.getWebSocketServer().send();(response.toString());
        LOGGER.info("Exiting election handling thread of node " + host.getNodeName());
    }

    public void setLeader(String leader) {
        this.leader = leader;
        JSONObject response = new JSONObject()
                .put("NODE_NAME", host.getNodeName())
                .put("SET_LEADER", true)
                .put("LEADER", leader)
                .put("GROUP_ID", groupID);
        host.getWSCommonConnection().send(response.toString());
    }

    public void setGroupID (String groupID) {
        this.groupID = groupID;
        JSONObject response = new JSONObject()
                .put("NODE_NAME", host.getNodeName())
                .put("ACTING_AS", MiddlewareType.FOLLOWER)
                .put("GROUP_ID", groupID);
        host.getWSCommonConnection().send(response.toString());
    }

    public String getGroupID() {
        return groupID;
    }

    public void addGroupMembers (int bullyId, String groupMember) {
        groupMembers.put(bullyId, groupMember);
        JSONObject response = new JSONObject()
                .put("NODE_NAME", host.getNodeName())
                .put("ACTING_AS", MiddlewareType.FOLLOWER)
                .put("ADD_GROUP_MEMBER", groupMember)
                .put("BULLY_ID", bullyId);
        host.getWSCommonConnection().send(response.toString());
    }

    @Override
    public MiddlewareType getMiddlewareType() {
        return MiddlewareType.FOLLOWER;
    }
    public synchronized void stopProcess() {
        messageReceivingBlockingQueue.add(new Message(MessageType.INTERRUPT, null)); // Poison pill strategy
        messageSendingBlockingQueue.add(new Message(MessageType.INTERRUPT, null)); // Poison pill strategy
        while (timerThread.isAlive()) {
            timerThreadInterrupted.set(true);
            timerThread.interrupt();
        };
        processIsActive.set(false);

        JSONObject response = new JSONObject()
                .put("NODE_NAME", host.getNodeName())
                .put("STOPPED_FOLLOWER_PROCESS", true);
        host.getWSCommonConnection().send(response.toString());

        LOGGER.info(host.getNodeName() + " follower process shutting down");
    }

    @Override
    public synchronized void startProcess() {
        processIsActive.set(true);
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
        }, this.host.getNodeName() + "-Follower-messageReceivingThread");
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
        }, this.host.getNodeName() + "-Follower-messageSendingThread");

        messageReceivingThread.start();
        messageSendingThread.start();
        timerThread.start();

        JSONObject response = new JSONObject()
                .put("NODE_NAME", host.getNodeName())
                .put("STARTED_FOLLOWER_PROCESS", true);
        host.getWSCommonConnection().send(response.toString());
    }

    @Override
    public void handle(Message message) throws InterruptedException {
        HashMap<String, String> fields = message.getFields();
        switch (message.getType().name()) {
            case "ASSIGN" :
                if(fields.get("TYPE").equals("LEADER")) {
                    // graceful termination of follower processes
                    LeaderMiddleware leaderMiddleware = new LeaderMiddleware(host);
                    leaderMiddleware.setGroupID(fields.get("GROUP_ID"));
                    if(fields.containsKey("FOLLOWERS")) {
                        String[] followers = fields.get("FOLLOWERS").split(",");
                        for (String follower : followers) {
                            int bullyID = Integer.parseInt(follower.split("=")[0]);
                            String name = follower.split("=")[1];
                            leaderMiddleware.addFollower(bullyID, name);
                        }
                    }
                    host.stopRunningMiddlewareProcessGracefully();
                    host.setMiddleware(leaderMiddleware);
                    host.startNewMiddlewareProcess();
                    LOGGER.info(host.getNodeName() + " Assigned as Leader");
                }
                break;
            case "TASK" :
                LOGGER.info(host.getNodeName() + " " + "Task received for follower");
                break;
            case "ELECTION" :
                synchronized (lock) {

                    LOGGER.info("from " + fields.get("SENDER") + " ELECTION message received for " + host.getNodeName());

                    JSONObject response = new JSONObject()
                            .put("MESSAGE_TYPE", "ELECTION")
                            .put("NODE_NAME", host.getNodeName())
                            .put("ELECTION_RECEIVED", true)
                            .put("ELECTION_SENDER", fields.get("SENDER"));
                    Thread.sleep(1500);
                    host.getWSCommonConnection().send(response.toString());

                    int electionInitiatorBullyID = Integer.parseInt(fields.get("CANDIDATE_BULLY_ID"));
                    if (electionInitiatorBullyID < host.getNodeBullyID()) {
                        message = new Message(MessageType.OK, fields.get("SENDER"));
                        host.sendMessage(message);

                        if (!electionInProgress.get() && !isCandidate.get() && processIsActive.get()) {

                            response = new JSONObject()
                                    .put("MESSAGE_TYPE", "ELECTION")
                                    .put("NODE_NAME", host.getNodeName())
                                    .put("IN_ELECTION", true)
                                    .put("IS_CANDIDATE", true);
                            host.getWSCommonConnection().send(response.toString());
                            Thread.sleep(1500);

                            isCandidate.set(true);
                            electionInProgress.set(true);

                            LOGGER.info(host.getNodeName() + " in a election");

                            List<Map.Entry<Integer, String>> bullies = groupMembers
                                    .entrySet()
                                    .stream()
                                    .filter(
                                            groupMemberIDAndNodeAddress -> groupMemberIDAndNodeAddress.getKey() > this.host.getNodeBullyID()
                                    )
                                    .collect(Collectors.toList());
                            for (Map.Entry<Integer, String> bullyIDAndAddress : bullies) {
                                message = new Message(MessageType.ELECTION, bullyIDAndAddress.getValue());
                                message.addField("CANDIDATE_BULLY_ID", String.valueOf(host.getNodeBullyID()));

                                host.sendMessage(message);
                            }

                            electionDecisionCommunicationBlockinqQueue.clear();
                            Thread electionHandlingThread = new Thread(this::handleElection, this.host.getNodeName() + "-Election-ElectionHandlingThread");
                            electionHandlingThread.start();

                        } else {
                            if (!processIsActive.get()) {
                                LOGGER.info(host.getNodeName() + " <- this node has not started Election even after received ELECTION from " + fields.get("SENDER") + " since follower middleware process is deactivated");
                            }
                            LOGGER.info(host.getNodeName() + " <- this node has Election is already in progress or moved to a leader process  or/and this node is a candidate when receiving ELECTION from " + fields.get("SENDER"));
                        }

                    }
                    break;
                }
            case "OK" :
                synchronized (lock) {
                    if (electionInProgress.get()) {
                        Message message1 = new Message(MessageType.INTERRUPT, "internal");
                        message1.addField("MESSAGE_TYPE", MessageType.OK.name()).addField("SENDER", fields.get("SENDER"));
                        electionDecisionCommunicationBlockinqQueue.add(message1);
                    }

                    isCandidate.set(false);
                    timer.reset();

                    JSONObject response = new JSONObject()
                            .put("MESSAGE_TYPE", "OK")
                            .put("IN_ELECTION", electionInProgress.get())
                            .put("NODE_NAME", host.getNodeName());
                    host.getWSCommonConnection().send(response.toString());
                    Thread.sleep(1500);

                    LOGGER.info(host.getNodeName() + " follower received OK for from " + fields.get("SENDER"));
                    break;
                }
            case "COORDINATOR" :
                synchronized (lock) {
                    if (electionInProgress.get()) {
                        Message message1 = new Message(MessageType.INTERRUPT, "internal");
                        message1.addField("MESSAGE_TYPE", MessageType.COORDINATOR.name()).addField("SENDER", fields.get("SENDER"));
                        electionDecisionCommunicationBlockinqQueue.add(message1);
                    }

                    timer.reset();
                    electionInProgress.set(false);

                    JSONObject response = new JSONObject()
                            .put("MESSAGE_TYPE", "COORDINATOR")
                            .put("NODE_NAME", host.getNodeName());
                    host.getWSCommonConnection().send(response.toString());

                    Thread.sleep(1500);

                    LOGGER.info(host.getNodeName() + " exited from the election since COORDINATOR received");
                    if (host.getNodeName().equals(message.getRecipientOrGroupID())) {
                        this.setLeader(fields.get("NEW_LEADER"));
                    } else {
                        System.out.println(message.getRecipientOrGroupID());
                    }
                    LOGGER.info(host.getNodeName() + " this follower received Coordinator from " + fields.get("SENDER") + " : NEW_LEADER is " +  this.leader);
                    break;
                }
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
            case "BEACON" :
                // reset timer
                synchronized(lock) {
                    if (electionInProgress.get()) {
                        Message message1 = new Message(MessageType.INTERRUPT, "internal");
                        message1.addField("MESSAGE_TYPE", MessageType.BEACON.name()).addField("SENDER", fields.get("SENDER"));
                        electionDecisionCommunicationBlockinqQueue.add(message1);
                    }
                    electionInProgress.set(false);
                    timer.reset();
                    JSONObject response = new JSONObject()
                            .put("MESSAGE_TYPE", "BEACON")
                            .put("NODE_NAME", host.getNodeName())
                            .put("ACTING_AS", MiddlewareType.FOLLOWER)
                            .put("BEACON_SENT_TO", this.host.getNodeName());
                    host.getWSCommonConnection().send(response.toString());
                    LOGGER.info("Follower " + host.getNodeName() + " : Beacon received from the leader : " + leader);
                    break;
                }
            default :
                fields.forEach((s, s2) -> LOGGER.info(s+":"+s2));

        }
    }

    @Override
    public void receiveMessage(Message message) {
        if (processIsActive.get())
            messageReceivingBlockingQueue.add(message);
    }


    @Override
    public void sendMessage(String recipientAddress, Message message) {
        if (processIsActive.get())
            messageSendingBlockingQueue.add(message);
    }

}

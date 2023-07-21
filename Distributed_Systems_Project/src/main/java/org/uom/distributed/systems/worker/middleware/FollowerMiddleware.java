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
        this.timerThread = new Thread(() -> {
            while (!timerThreadInterrupted.get()) {
                timer.start();
                synchronized (lock) {
                    LOGGER.info(host.getStateType() + " node " + host.getNodeName() + " received beacon " + 15 + " seconds ago."
                                    + " assuming leader is unresponsive."
                    );

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

                        for (Map.Entry<Integer, String> innocentIDAndNodeAddress : groupMembers.entrySet()) {
                            message = new Message(MessageType.OK, innocentIDAndNodeAddress.getValue());
                            host.sendMessage(message);

                            message = new Message(MessageType.COORDINATOR, innocentIDAndNodeAddress.getValue());
                            message.addField("NEW_LEADER", this.host.getNodeName());
                            host.sendMessage(message);
                        }

                        StringBuilder followers = new StringBuilder();
                        groupMembers.values().stream().iterator().forEachRemaining((i) -> {
                            followers.append(i);
                            followers.append(",");
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
                            // there are bully ids greater than mine and start the election
                            for (Map.Entry<Integer, String> bullyIDAndAddress : bullies) {
                                message = new Message(MessageType.ELECTION, bullyIDAndAddress.getValue());
                                message.addField("CANDIDATE_BULLY_ID", String.valueOf(host.getNodeBullyID()));

                                host.sendMessage(message);
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
        Message message;
        try {
            message = electionDecisionCommunicationBlockinqQueue.poll(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (isCandidate.get() && electionInProgress.get() && message == null) {
            LOGGER.info("Elected this node " + host.getNodeName() + " since still no OK or COORDINATOR message received");

            StringBuilder followers = new StringBuilder();
            groupMembers.values().stream().iterator().forEachRemaining((i) -> {
                followers.append(i);
                followers.append(",");
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
                LOGGER.info(host.getNodeName() + " " + message + (isCandidate.get() ? " and still a candidate" : "") + (electionInProgress.get() ? " and still election is going on." : ""));
                LOGGER.info("Not Elected this node " + host.getNodeName() + " since " + message.getFields().get("MESSAGE_TYPE") + " received");
            } else {
                LOGGER.info(host.getNodeName() + " <- Not Elected this node since timeout and election is over or/and this node not a candidate ");
            }
        }
        electionInProgress.set(false);
        isCandidate.set(false);
        LOGGER.info("Exiting election handling thread of node " + host.getNodeName());
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
        while (timerThread.isAlive()) {
            timerThreadInterrupted.set(true);
            timerThread.interrupt();
        };
        processIsActive.set(false);
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
                            leaderMiddleware.addFollower(follower);
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

                    int electionInitiatorBullyID = Integer.parseInt(fields.get("CANDIDATE_BULLY_ID"));
                    if (electionInitiatorBullyID < host.getNodeBullyID()) {
                        message = new Message(MessageType.OK, fields.get("SENDER"));
                        host.sendMessage(message);

                        if (!electionInProgress.get() && !isCandidate.get() && processIsActive.get()) {
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

                    LOGGER.info(host.getNodeName() + " exited from the election since COORDINATOR received");
                    if (host.getNodeName().equals(message.getRecipientOrGroupID())) {
                        this.setLeader(fields.get("NEW_LEADER"));
                    }
                    LOGGER.info(host.getNodeName() + " this follower received Coordinator from " + fields.get("SENDER") + " : NEW_LEADER is " + this.leader);
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

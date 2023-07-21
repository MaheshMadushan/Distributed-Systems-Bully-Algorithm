package org.uom.distributed.systems.worker.middleware;

import org.uom.distributed.systems.Config;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FollowerMiddleware implements IMiddleware {
    private final Node host;
    private String groupID;
    private String leader;
    private final HashMap<Integer, String> groupMembers;
    private final BlockingQueue<Message> messageReceivingBlockingQueue
            = new LinkedBlockingQueue<>();
    private final BlockingQueue<Message> messageSendingBlockingQueue
            = new LinkedBlockingQueue<>();
    private final AtomicInteger counter = new AtomicInteger(15);
    private final AtomicBoolean timerInterrupted = new AtomicBoolean(false);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean isCandidate = new AtomicBoolean(false);
    private final AtomicBoolean processIsActive = new AtomicBoolean(false);
    private final Thread timerThread;
    private final Object lock = new Object();


    public FollowerMiddleware(Node host) {
        this.host = host;
        this.groupMembers = new HashMap<>(10);
        this.timerThread = new Thread(() -> {
            while (!timerInterrupted.get()) {
                try {
                    Thread.sleep(Config.UNIT_TIME);
                    counter.decrementAndGet();
                    if (counter.get() == 0) {
                        synchronized (lock) {
                            if (counter.get() == 0) {
                                System.out.println(
                                        host.getStateType() + " node " + host.getNodeName() + " received beacon " + (15 - counter.get()) + " seconds ago."
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

                                if (groupMembers.isEmpty()) {
                                    // assigning itself as the leader since no other group members
                                    message = new Message(MessageType.ASSIGN, host.getNodeName());
                                    message.addField("TYPE", MiddlewareType.LEADER.toString());
                                    message.addField("GROUP_ID", groupID);
                                    System.out.println(message);
                                    host.sendMessage(message);
                                    timerInterrupted.set(true);
                                } else if (bullies.isEmpty()) {
                                    // im the leader since no bully id greater than mine is presents
                                    isCandidate.set(false);
                                    for (Map.Entry<Integer, String> innocentIDAndNodeAddress : groupMembers.entrySet()) {
                                        message = new Message(MessageType.COORDINATOR, innocentIDAndNodeAddress.getValue());
                                        message.addField("NEW_LEADER", this.host.getNodeName());
                                        host.sendMessage(message);
                                    }
                                } else {
                                    electionInProgress.set(true);
                                    isCandidate.set(true);
                                    // there are bully ids greater than mine and start the election
                                    for (Map.Entry<Integer, String> bullyIDAndAddress : bullies) {
                                        message = new Message(MessageType.ELECTION, bullyIDAndAddress.getValue());
                                        message.addField("CANDIDATE_BULLY_ID", String.valueOf(host.getNodeBullyID()));
                                        message.addField("SENDER", host.getNodeName());
                                        System.out.println(message);
                                        host.sendMessage(message);
                                    }

                                    Thread electionHandlingThread = new Thread(this::handleElection);

                                    electionHandlingThread.start();
                                }
                            }
                        }
                        while (electionInProgress.get()) {

                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("timer thread is shutting down");
                }
            }
        });

    }

    private void handleElection() {

        // let 5 seconds for bully to answer
        // if not self promote to leader
        System.out.println("Election handling thread started for node " + host.getNodeName());
        for (int i = 0; i < 5; i++) {
            try {
                Thread.sleep(Config.UNIT_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (isCandidate.get() && electionInProgress.get()) {
            System.out.println("Elected this node " + host.getNodeName());
            Message assignMessage = new Message(MessageType.ASSIGN, host.getNodeName());
            assignMessage.addField("TYPE", MiddlewareType.LEADER.toString());
            assignMessage.addField("GROUP_ID", groupID);
            StringBuilder followers = new StringBuilder();
            groupMembers.values().stream().iterator().forEachRemaining((i) -> {
                followers.append(i);
                followers.append(",");
            });
            assignMessage.addField("FOLLOWERS", followers.toString());
            System.out.println(assignMessage);
            try {
                host.sendMessage(assignMessage);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (Map.Entry<Integer, String> innocentIDAndNodeAddress : groupMembers.entrySet()) {
                Message coordMessage = new Message(MessageType.COORDINATOR, innocentIDAndNodeAddress.getValue());
                coordMessage.addField("NEW_LEADER", this.host.getNodeName());
                try {
                    host.sendMessage(coordMessage);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } else {
            System.out.println("Not Elected this node " + host.getNodeName());
        }
        System.out.println("Exiting election handling thread of node " + host.getNodeName());
        isCandidate.set(false);
        electionInProgress.set(false);

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
    public void stopProcess() {
        messageReceivingBlockingQueue.add(new Message(MessageType.INTERRUPT, null)); // Poison pill strategy
        messageSendingBlockingQueue.add(new Message(MessageType.INTERRUPT, null)); // Poison pill strategy
        while (timerThread.isAlive()) {
            timerInterrupted.set(true);
            timerThread.interrupt();
        };
        processIsActive.set(false);
    }

    @Override
    public void startProcess() {
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
                        String[] a = fields.get("FOLLOWERS").split(",");
                        for (String g : a) {
                            leaderMiddleware.addFollower(g);
                        }
                    }
                    host.stopRunningMiddlewareProcessGracefully();
                    host.setMiddleware(leaderMiddleware);
                    host.startNewMiddlewareProcess();
                    System.out.println(host.getNodeName() + " Assigned as Leader");
                }
                break;
            case "TASK" :
                System.out.println(host.getNodeName() + " " + "Task received for follower");
                break;
            case "ELECTION" :
                synchronized (lock) {
                    if (Integer.parseInt(fields.get("CANDIDATE_BULLY_ID")) < host.getNodeBullyID()) {
                        message = new Message(MessageType.OK, fields.get("SENDER"));
                        message.addField("NEW_LEADER", this.host.getNodeName());
                        host.sendMessage(message);

                        if (!electionInProgress.get() && !isCandidate.get()) {
                            isCandidate.set(true);
                            electionInProgress.set(true);
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
                                message.addField("SENDER", host.getNodeName());
                                System.out.println(message);
                                host.sendMessage(message);
                            }

                            Thread electionHandlingThread = new Thread(this::handleElection);

                            electionHandlingThread.start();
                        } else {
                            System.out.println("Election is already in progress in " + host.getNodeName() + " and this node is a candidate when receiving ELECTION message");
                        }

                    } else {
                        throw new RuntimeException("Critical error. Candidate BullyIDs are equal or greater than this node bully id. system shutting down");
                    }
                    System.out.println(host.getNodeName() + " Election received for follower");
                    break;
                }
            case "OK" :
                isCandidate.set(false);
                System.out.println(host.getNodeName() + " OK received for follower");
                break;
            case "COORDINATOR" :
                synchronized (lock) {
                    electionInProgress.set(false);
                    counter.set(15);
                    if (host.getNodeName().equals(message.getRecipientOrGroupID())) {
                        this.setLeader(fields.get("NEW_LEADER"));
                    }
                    System.out.println(host.getNodeName() + " Coordinator received for follower : NEW_LEADER is " + this.leader);
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

                System.out.println("adding member " + newGroupMemberAddress + " with bully id " + newGroupMemberBullyID + " to node " + host.getNodeName() + " as group member.");
                break;
            case "BEACON" :
                // reset counter
                counter.set(15);
                System.out.println("Follower " + host.getNodeName() + " : Beacon received from the leader : " + leader);
                break;
            default :
                fields.forEach((s, s2) -> System.out.println(s+":"+s2));

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

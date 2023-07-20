package org.uom.distributed.systems.worker.middleware;

import org.uom.distributed.systems.Config;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.worker.IMiddleware;
import org.uom.distributed.systems.worker.Node;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FollowerMiddleware implements IMiddleware {
    private final Node host;
    private String groupID;
    private String leader;
    private final HashMap<Integer, String> groupMembers;
    private final BlockingQueue<Message> messageReceivingBlockingQueue
            = new ArrayBlockingQueue<>(10);
    private final BlockingQueue<Message> messageSendingBlockingQueue
            = new ArrayBlockingQueue<>(10);
    private AtomicInteger counter = new AtomicInteger(15);
    private AtomicBoolean timerInterrupted = new AtomicBoolean(false);
    private final Thread timerThread;


    public FollowerMiddleware(Node host) {
        this.host = host;
        this.groupMembers = new HashMap<>(10);
        this.timerThread = new Thread(() -> {
            while (!timerInterrupted.get()) {
                try {
                    Thread.sleep(Config.UNIT_TIME);
                    if(counter.decrementAndGet() == 0) {
                        System.out.println(
                                "node " + host.getNodeName() + " " + host.getStateType() + " received beacon " + (15 - counter.get()) + " seconds ago.");
                        // leader is not responding
                        // start an election message
                        // while election wait
                        List<Map.Entry<Integer, String>> bullies = groupMembers
                                .entrySet()
                                .stream()
                                .filter(
                                        groupMemberIDAndNodeAddress -> groupMemberIDAndNodeAddress.getKey() > this.host.getNodeBullyID()
                                )
                                .collect(Collectors.toList());

                        Message message;

                        if(groupMembers.isEmpty()) {
                            message = new Message(MessageType.ASSIGN, host.getNodeName());
                            message.addField("TYPE", MiddlewareType.LEADER.toString());
                            message.addField("GROUP_ID", groupID);
                            System.out.println(message);
                            host.sendMessage(message);
                        }
                        else if (bullies.isEmpty()) {
                            // im the leader
                            for (Map.Entry<Integer, String> innocentIDAndNodeAddress: groupMembers.entrySet()){
                                message =
                                        new Message(MessageType.COORDINATOR, innocentIDAndNodeAddress.getValue());
                                System.out.println(message);
                                host.sendMessage(message);
                            }
                        } else {
                            for (Map.Entry<Integer, String> bullyIDAndAddress : bullies) {
                                message =
                                        new Message(MessageType.ELECTION, bullyIDAndAddress.getValue());
                                System.out.println(message);
                                host.sendMessage(message);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    System.out.println("timer thread is shutting down");
                }

            }
        });
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
                    host.stopRunningMiddlewareProcessGracefully();
                    host.setMiddleware(leaderMiddleware);
                    host.startNewMiddlewareProcess();
                    System.out.println(host.getNodeName() + " " + "Assigned as Leader");
                }
                else {
                    System.out.println("message is discarded");
                }
                break;
            case "TASK" :
                System.out.println(host.getNodeName() + " " + "Task received for follower");
                break;
            case "ELECTION" :
                System.out.println(host.getNodeName() + " " + "Election received for follower");
                break;
            case "OK" :
                System.out.println(host.getNodeName() + " " + "OK received for follower");
                break;
            case "COORDINATOR" :
                System.out.println(host.getNodeName() + " " + "Coordinator received for follower");
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

                System.out.println("adding group member");
                break;
            case "BEACON" :
                // reset counter
                counter.set(15);
//                System.out.println("Follower " + host.getNodeName() + " : Beacon received from the leader : " + leader);
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

}

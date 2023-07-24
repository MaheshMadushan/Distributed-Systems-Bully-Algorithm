package org.uom.distributed.systems.Utilities;

import org.java_websocket.WebSocket;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageService;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.registry.Registry;
import org.uom.distributed.systems.worker.Node;
import org.uom.distributed.systems.worker.middleware.MiddlewareType;

import java.util.*;

public class NodeManager {
    private final double RADIUS = 20.0;
    private List<Node> NODE_LIST = new ArrayList<>(10);
    private final Map<Double, Node> LEADER_ELIGIBILITY_MAP = new TreeMap<>(Collections.reverseOrder());
    private final List<Node> leaders = new ArrayList<>();
    private final List<Thread> runningNodes = new ArrayList<>();
    private final MessageService MESSAGE_SERVICE = new MessageService();
    private WebSocket common_conn;
    private WebSocket log_conn;
    private void startNodes() {
        for (Node node : NODE_LIST) {
            Thread nodeThread = new Thread(node);
            nodeThread.start();
            runningNodes.add(nodeThread);
        }
    }

    public NodeManager(WebSocket common_conn, WebSocket log_conn) {
        this.common_conn = common_conn;
        this.log_conn = log_conn;
    }


    private void addNodesToList(List<List<Integer>> nodes) {
        for (List<Integer> ints : nodes) {
            short ENERGY_LEVEL = 2;
            short y = 1;
            short x = 0;
            System.out.printf("Provisioning Node: X=%d | Y=%d | Energy Level=%d \n", ints.get(x), ints.get(y), ints.get(ENERGY_LEVEL));
            NODE_LIST.add(new Node(ints.get(x), ints.get(y), ints.get(ENERGY_LEVEL), common_conn, log_conn));
        }
    }

    public void initiateSystem(List<List<Integer>> nodes) throws InterruptedException {
        addNodesToList(nodes);
        registerNodesInRegistry();
        determineEligibleNeighboursForNodes();
        startNodes();
        leaderElection();
    }

    private double calculatedEuclideanDistance (Node node1, Node node2) {
        double euclideanDistanceSquared = Math.pow(node1.getX() - node2.getX(), 2) + Math.pow(node1.getY() - node2.getY(), 2);
        return Math.pow(euclideanDistanceSquared, 0.5);
    }


    private void registerNodesInRegistry() {
        for (Node node : NODE_LIST) {
            Registry.registerNode(node);
        }
    }

    private void determineEligibleNeighboursForNodes() {
        for (int i = 0; i < NODE_LIST.size(); i++) {
            Node node_a = NODE_LIST.get(i);
            for (int j = i; j < NODE_LIST.size(); j++) {
                Node node_b = NODE_LIST.get(j);
                double euclideanDistance = calculatedEuclideanDistance(node_a, node_b);
                if (euclideanDistance <= RADIUS && euclideanDistance > 0) {
                    node_a.addNeighbour(node_b);
                    node_b.addNeighbour(node_a);
                }
            }
        }
        // Test
        for (Node node : NODE_LIST) {
            System.out.println(node.toString());
        }
    }

    private void leaderElection() {
        createClusters();
    }

    private void createClusters() {
        int conflictResoluter = 0;
        for (Node node : NODE_LIST) {

            int numOfNeighbours = node.getCountOfNeighbours();
            int energyLevelOfTheNode = node.getEnergyLevel();

            double idealEnergyPerNode /* the ratio */ = (double) energyLevelOfTheNode / (numOfNeighbours + 1); // counts self node

            if (LEADER_ELIGIBILITY_MAP.containsKey(idealEnergyPerNode)) {
                conflictResoluter++;
                LEADER_ELIGIBILITY_MAP.put(idealEnergyPerNode + conflictResoluter, node); // conflict resolution (given priority as they have arrived (FCFS))
            } else {
                LEADER_ELIGIBILITY_MAP.put(idealEnergyPerNode, node);
            }
        }

        // Assign BullyID to nodes to use in bully algorithm
        int bullyID = LEADER_ELIGIBILITY_MAP.size() - 1;
        for (Map.Entry<Double, Node> entry : LEADER_ELIGIBILITY_MAP.entrySet()) {
            entry.getValue().setNodeBullyID(bullyID--);
        }

        // Clustering with leader election
        for (Double key : LEADER_ELIGIBILITY_MAP.keySet()) {

            // LEADER_ELIGIBILITY_MAP - sorted treemap according to "the ratio"
            Node node = LEADER_ELIGIBILITY_MAP.get(key);

            // skip followers that already in a group or already a leader of a group from leader election
            if (node.getStateType().equals(MiddlewareType.IDLE)){

                // set node with the highest ratio as the leader - means first element is always a leader
                // This is a leader - form a new cluster around this leader
                String groupID = UUID.randomUUID().toString();

                Message message = new Message(MessageType.ASSIGN, node.getNodeName());
                message.addField("TYPE", MiddlewareType.LEADER.toString());
                message.addField("GROUP_ID", groupID);
                MESSAGE_SERVICE.sendMessage(message);
                leaders.add(node);

                // set that node's neighbours as followers - forming the group
                for (Map.Entry<String, Node> entry : node.getNeighbours().entrySet()) {
                    Node neighbour = entry.getValue();

                    if (neighbour.getStateType().equals(MiddlewareType.IDLE)) {
                        message = new Message(MessageType.ASSIGN, neighbour.getNodeName());
                        message.addField("TYPE", MiddlewareType.FOLLOWER.toString());
                        message.addField("LEADER", node.getNodeName());
                        message.addField("GROUP_ID", groupID);
                        MESSAGE_SERVICE.sendMessage(message);

                        message = new Message(MessageType.ADD_FOLLOWER, node.getNodeName());
                        message.addField("FOLLOWER_NAME", neighbour.getNodeName());
                        message.addField("FOLLOWER_BULLY_ID", String.valueOf(neighbour.getNodeBullyID()));
                        MESSAGE_SERVICE.sendMessage(message);
                    }
                }
            }
            // continue - if there is left-out nodes then get node with the highest ratio make it as another leader (form new group around that)
        }

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            leaders.get(0).kill();

        });
        thread.start();
    }

    public void killSystem() {
        for (Thread nodeThread : runningNodes) {
            nodeThread.interrupt();
        }
        for (Node node : NODE_LIST) {
            node.kill();
        }
    }

}

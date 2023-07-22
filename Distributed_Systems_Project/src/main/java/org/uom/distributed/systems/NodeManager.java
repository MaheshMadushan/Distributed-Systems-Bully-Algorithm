package org.uom.distributed.systems;

import org.java_websocket.server.WebSocketServer;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageService;
import org.uom.distributed.systems.messaging.MessageType;
import org.uom.distributed.systems.registry.Registry;
import org.uom.distributed.systems.worker.Node;
import org.uom.distributed.systems.worker.middleware.MiddlewareType;

import java.net.InetSocketAddress;
import java.util.*;

public class NodeManager {
    private static final short X = 0;
    private static final short Y = 1;
    private static final short ENERGY_LEVEL = 2;
    private static final double RADIUS = 20.0;
    private static List<Node> NODE_LIST = new ArrayList<>(10);
    private static final Map<Double, Node> LEADER_ELIGIBILITY_MAP = new TreeMap<>(Collections.reverseOrder());
    private static final List<Node> leaders = new ArrayList<>();
    private static final MessageService MESSAGE_SERVICE = new MessageService();
    private static WebSocketServer server = null;

    public NodeManager(WebSocketServer server) {
        this.server = server;
    }

    public static void initiateSystem(int[][] inputs) throws InterruptedException {
        addNodesToList(inputs);
        registerNodesInRegistry();
        determineEligibleNeighboursForNodes();
        startNodes();
        leaderElection();
    }

    public static void initiateSystem(Node[] inputs) throws InterruptedException {
        NODE_LIST = Arrays.asList(inputs);
        registerNodesInRegistry();
        determineEligibleNeighboursForNodes();
        startNodes();
        leaderElection();
    }

    private static void startNodes() {
        for (Node node : NODE_LIST) {
            new Thread(node).start();
        }
    }

    private static double calculatedEuclideanDistance (Node node1, Node node2) {
        double euclideanDistanceSquared = Math.pow(node1.getX() - node2.getX(), 2) + Math.pow(node1.getY() - node2.getY(), 2);
        return Math.pow(euclideanDistanceSquared, 0.5);
    }

    private static void addNodesToList(int[][] inputs) {
        for (int[] ints : inputs) {
            System.out.printf("Provisioning Node: X=%d | Y=%d | Energy Level=%d \n", ints[X], ints[Y], ints[ENERGY_LEVEL]);
            NODE_LIST.add(new Node(ints[X], ints[Y], ints[ENERGY_LEVEL],server));
        }
    }

    private static void registerNodesInRegistry() {
        for (Node node : NODE_LIST) {
            Registry.registerNode(node);
        }
    }

    private static void determineEligibleNeighboursForNodes() {
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

    private static void leaderElection() throws InterruptedException {
        createClusters();
    }

    private static void createClusters() throws InterruptedException {
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
        int bullyID = LEADER_ELIGIBILITY_MAP.size();
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

}

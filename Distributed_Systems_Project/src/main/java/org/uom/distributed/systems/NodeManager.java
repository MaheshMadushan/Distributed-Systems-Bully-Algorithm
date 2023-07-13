package org.uom.distributed.systems;

import org.uom.distributed.systems.worker.Node;
import org.uom.distributed.systems.worker.designation.FollowerNode;
import org.uom.distributed.systems.worker.designation.LeaderNode;

import java.util.*;

public class NodeManager {
    private static final short X = 0;
    private static final short Y = 1;
    private static final short ENERGY_LEVEL = 2;
    private static final double RADIUS = 20.0;
    private static final List<Node> NODE_LIST = new ArrayList<>(10);
    private static final Map<Double, Node> LEADER_ELIGIBILITY_MAP = new TreeMap<>(Collections.reverseOrder());
    private static final List<LeaderNode> leaders = new ArrayList<>();

    public static void initiateSystem(int[][] inputs) {
        addNodesToList(inputs);
        determineEligibleNeighboursForNodes();
        leaderElection();
        // startNodes();
    }

    private static double calculatedEuclideanDistance (Node node1, Node node2) {
        double euclideanDistanceSquared = Math.pow(node1.getX() - node2.getX(), 2) + Math.pow(node1.getY() - node2.getY(), 2);
        return Math.pow(euclideanDistanceSquared, 0.5);
    }

    private static void addNodesToList(int[][] inputs) {
        for (int[] ints : inputs) {
            System.out.printf("Provisioning Node: X=%d | Y=%d | Energy Level=%d \n", ints[X], ints[Y], ints[ENERGY_LEVEL]);
            NODE_LIST.add(new Node(ints[X], ints[Y], ints[ENERGY_LEVEL]));
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

    private static void leaderElection() {
        createClusters();
    }

    private static void createClusters() {
        for (Node node : NODE_LIST) {

            int numOfNeighbours = node.getCountOfNeighbours();
            int energyLevelOfTheNode = node.getEnergyLevel();

            double idealEnergyPerNode /* the ratio */ = (double) energyLevelOfTheNode / (numOfNeighbours + 1); // counts self node

            if (LEADER_ELIGIBILITY_MAP.containsKey(idealEnergyPerNode)) {
                LEADER_ELIGIBILITY_MAP.put(idealEnergyPerNode + 1, node); // conflict resolution (given priority as they have arrived (FCFS))
            } else {
                LEADER_ELIGIBILITY_MAP.put(idealEnergyPerNode, node);
            }
        }

        // Assign BullyID to nodes to use in bully algorithm
        int bullyID = 0;
        for (Map.Entry<Double, Node> entry : LEADER_ELIGIBILITY_MAP.entrySet()) {
            entry.getValue().setNodeBullyID(bullyID++);
        }

        // Clustering with leader election
        for (Double key : LEADER_ELIGIBILITY_MAP.keySet()) {

            // LEADER_ELIGIBILITY_MAP - sorted treemap according to "the ratio"
            Node node = LEADER_ELIGIBILITY_MAP.get(key);

            // skip followers that already in a group or already a leader of a group from leader election
            if (!isLeader(node) && !isFollower(node)){

                // set node with the highest ratio as the leader - means first element is always a leader
                // This is a leader - form a new cluster around this leader
                String groupID = UUID.randomUUID().toString();
                LeaderNode leaderNode = new LeaderNode(node, leaders);
                leaderNode.setGroupID(groupID);

                List<FollowerNode> groupMembers = new ArrayList<>(10);

                // set that node's neighbours as followers - forming the group
                for (Map.Entry<String, Node> entry : node.getNeighbours().entrySet()) {
                    Node neighbour = entry.getValue();

                    if (!isFollower(neighbour)) {
                        FollowerNode followerNode = new FollowerNode(neighbour, groupMembers);
                        followerNode.setGroupID(groupID);
                        followerNode.setLeader(leaderNode);

                        groupMembers.add(followerNode);
                        leaderNode.addFollower(followerNode);
                    }
                }
                leaders.add(leaderNode);
            }
            // continue - if there is left-out nodes then get node with the highest ratio make it as another leader (form new group around that)
        }
    }

    private static boolean isLeader (Node node) {
        for (LeaderNode leaderNode : leaders) {
            if (leaderNode.getNodeName().equals(node.getNodeName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean isFollower (Node node) {
        for (LeaderNode leaderNode : leaders) {
            if (leaderNode.isFollower(node)) {
                return true;
            }
        }
        return false;
    }

}

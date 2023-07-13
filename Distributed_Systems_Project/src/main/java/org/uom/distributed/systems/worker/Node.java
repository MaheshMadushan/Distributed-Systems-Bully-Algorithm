package org.uom.distributed.systems.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Node implements Runnable {
    private long nodeBullyID;
    private String nodeName;
    private int X_COORDINATE;
    private int Y_COORDINATE;
    private AtomicInteger ENERGY_LEVEL ;
    private final HashMap<String, Node> neighbours;
    public Node(int X, int Y, int ENERGY_LEVEL) {
        this.X_COORDINATE = X;
        this.Y_COORDINATE = Y;
        this.ENERGY_LEVEL = new AtomicInteger(ENERGY_LEVEL);
        this.neighbours = new HashMap<>(10);
        this.nodeName = String.format("%d_%d_%d",X,Y,ENERGY_LEVEL);
    };

    @Override
    public String toString() {
        StringBuilder stringNodeRep = new StringBuilder("Node{" +
                "nodeBullyID=" + nodeBullyID +
                ", nodeName='" + nodeName + '\'' +
                ", X_COORDINATE=" + X_COORDINATE +
                ", Y_COORDINATE=" + Y_COORDINATE +
                ", ENERGY_LEVEL=" + ENERGY_LEVEL +
                ", neighbours=");
        for (Map.Entry<String, Node> entry : neighbours.entrySet()) {
            stringNodeRep.append(entry.getKey());
            stringNodeRep.append(", ");
        }

        return stringNodeRep.toString();
    }

    public HashMap<String, Node> getNeighbours() {
        return neighbours;
    }

    public int getCountOfNeighbours() {
        return this.neighbours.size();
    }

    public void deleteNeighbour(Node node) {
        this.neighbours.remove(node.getNodeName(), node);
    }

    public void addNeighbour(Node node) {
        this.neighbours.put(node.getNodeName(), node);
    }

    public void setX(int X) {
        this.X_COORDINATE = X;
    }

    public void setY(int Y) {
        this.Y_COORDINATE = Y;
    }

    public void setEnergyLevel(int energyLevel) {
        this.ENERGY_LEVEL.set(energyLevel);
    }

    public int getX() {
        return X_COORDINATE;
    }

    public int getY() {
        return Y_COORDINATE;
    }

    public int getEnergyLevel() {
        return ENERGY_LEVEL.get();
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public long getNodeBullyID() {
        return nodeBullyID;
    }

    public void setNodeBullyID(long nodeBullyID) {
        this.nodeBullyID = nodeBullyID;
    }

    @Override
    public void run() {
        Thread workerThread = new Thread(() -> {

        });

        Thread messageProcessingThread = new Thread(() -> {

        });

        workerThread.start();
        messageProcessingThread.start();

        while (ENERGY_LEVEL.get() > 0) {
            // simulating node power consumption
            try {
                Thread.sleep(10000);
                ENERGY_LEVEL.decrementAndGet();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // send eviction message
    }
}

package org.uom.distributed.systems.worker;

import org.java_websocket.server.WebSocketServer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uom.distributed.systems.Config;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.messaging.MessageService;
import org.uom.distributed.systems.worker.middleware.IdleState;
import org.uom.distributed.systems.worker.middleware.MiddlewareType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Node implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);
    private long nodeBullyID;
    private String nodeName;
    private int X_COORDINATE;
    private int Y_COORDINATE;
    private final AtomicInteger ENERGY_LEVEL ;
    private final BlockingQueue<Message> taskQueue = new ArrayBlockingQueue<>(10);
    private final MessageService messageService = new MessageService();
    private final HashMap<String, Node> neighbours;
    private IMiddleware middleware = new IdleState(this);

    public WebSocketServer getWebSocketServer() {
        return webSocketServer;
    }

    private WebSocketServer webSocketServer;

    public Node(int X, int Y, int ENERGY_LEVEL, WebSocketServer server) {
        this.X_COORDINATE = X;
        this.Y_COORDINATE = Y;
        this.ENERGY_LEVEL = new AtomicInteger(ENERGY_LEVEL);
        this.neighbours = new HashMap<>(10);
        this.nodeName = String.format("%d_%d_%d",X,Y,ENERGY_LEVEL);
        this.webSocketServer = server;
        JSONObject response = new JSONObject()
                .put("MESSAGE_TYPE", "PROVISION")
                .put("X", X)
                .put("Y", Y)
                .put("STATUS", this.middleware.getMiddlewareType())
                .put("ENERGY_LEVEL", this.ENERGY_LEVEL.get())
                .put("NODE_NAME", this.nodeName);
        webSocketServer.broadcast(response.toString());
    };

    public Node(int X, int Y, int ENERGY_LEVEL) {
        this.X_COORDINATE = X;
        this.Y_COORDINATE = Y;
        this.ENERGY_LEVEL = new AtomicInteger(ENERGY_LEVEL);
        this.neighbours = new HashMap<>(10);
        this.nodeName = String.format("%d_%d_%d",X,Y,ENERGY_LEVEL);
    };

    public void setMiddleware(IMiddleware middleware) {
        this.middleware = middleware;
        JSONObject response = new JSONObject()
                .put("NODE_NAME", getNodeName())
                .put("STATUS", middleware.getMiddlewareType())
                .put("GROUP_ID", middleware.getGroupID());
        webSocketServer.broadcast(response.toString());
    }

    public MiddlewareType getStateType() {
        return middleware.getMiddlewareType();
    }

    @Override
    public String toString() {
        StringBuilder stringNodeRep = new StringBuilder("Node{" +
                "nodeBullyID=" + nodeBullyID +
                ", nodeName='" + nodeName + '\'' +
                ", X_COORDINATE=" + X_COORDINATE +
                ", Y_COORDINATE=" + Y_COORDINATE +
                ", ENERGY_LEVEL=" + ENERGY_LEVEL.get() +
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
        JSONObject response = new JSONObject()
                .put("NODE_NAME", getNodeName())
                .put("BULLY_ID", nodeBullyID);
        webSocketServer.broadcast(response.toString());
    }

    // simulates node's network interface
    public void receiveMessage(Message message) {
        middleware.receiveMessage(message);
    }

    public void sendMessage(Message message)  {
        if(ENERGY_LEVEL.addAndGet(-2) > 0) {
            message.addField("SENDER", this.getNodeName());
            LOGGER.info(message.toString());
            messageService.sendMessage(message);
        }
    }

    @Override
    public void run() {
        Thread workerThread = new Thread(() -> {
            try {
                while (true) {
                    taskQueue.take();
                }
            } catch (InterruptedException e) {
                LOGGER.info("node " + nodeName + " is shutting down");
            }
        }, this.getNodeName() + "-WorkerThread");

        workerThread.start();

        while (ENERGY_LEVEL.get() > 0) {
            // simulating node power consumption
            try {
                Thread.sleep(Config.UNIT_TIME);
                ENERGY_LEVEL.decrementAndGet();
//                JSONObject response = new JSONObject()
//                        .put("ENERGY_LEVEL", ENERGY_LEVEL.get())
//                        .put("NODE_NAME", this.nodeName);
//                webSocketServer.broadcast(response.toString());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        this.stopRunningMiddlewareProcessGracefully();
        workerThread.interrupt();
        LOGGER.info("node " + nodeName + " " + getStateType() + " evicted");
        JSONObject response = new JSONObject()
                .put("MESSAGE_TYPE", "EVICTION")
                .put("EVICTED", true)
                .put("NODE_NAME", this.nodeName);
        webSocketServer.broadcast(response.toString());
    }

    public void stopRunningMiddlewareProcessGracefully() {
        middleware.stopProcess();
    }

    public void startNewMiddlewareProcess() {
        middleware.startProcess();
    }

    public void kill() {
        this.setEnergyLevel(0);
    }
}

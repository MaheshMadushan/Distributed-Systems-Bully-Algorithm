package org.uom.distributed.systems.server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONObject;
import org.uom.distributed.systems.Utilities.NodeManager;

public class MonitoringServer extends WebSocketServer {
    private static int client_count = 0;
    private static AtomicBoolean systemIsRunning = new AtomicBoolean(false);
    private static final HashMap<String, WebSocket> webSocketHashMap = new HashMap<>(2);
    private NodeManager nodeManager;
    Thread system = new Thread(() -> {
        systemIsRunning.set(true);
        nodeManager = new NodeManager(webSocketHashMap.get("COMMON"), webSocketHashMap.get("LOG"));

        int[][] input = {
                {2, 3, 345}, {12, 45, 100}, {35, 45, 533}, {1, 38, 100}, {4, 5, 20}, {50, 3, 98}, {22, 2, 144}, {34, 33, 233}, {11, 39, 101}, {4, 1, 4000}, {4, 2, 4000}, {4, 3, 4000}, {3, 4, 4000}, {2, 4, 4000}, {1, 4, 4000}
        };
//            int[][] input = {
//                    {4, 1, 4000}, {4, 2, 4000}, {4, 3, 4000}, {3, 4, 4000}, {2, 4, 4000}, {1, 4, 4000}
//            };

        try {
            nodeManager.initiateSystem(input);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    });

    public MonitoringServer(InetSocketAddress address) {
        super(address);
    }
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("connected");
    }
    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("connection closed by the browser");
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        JSONObject json = new JSONObject(message);
        webSocketHashMap.put((String) json.get("SOCKET"), conn);
        if (webSocketHashMap.containsKey("LOG") && webSocketHashMap.containsKey("COMMON") && !systemIsRunning.get()) {
            system.start();
        } else if(systemIsRunning.get()){
            System.out.println("hi");
            conn.send(String.valueOf(new JSONObject().put("MESSAGE_TYPE","ERROR").put("MESSAGE","Old system is still running. please restart the server and try again.")));
        }
    }

    @Override
    public void onMessage( WebSocket conn, ByteBuffer message ) {
        System.out.println("received ByteBuffer from " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("an error occurred on connection " + conn.getRemoteSocketAddress()  + ":" + ex);
    }

    @Override
    public void onStart() {
        System.out.println("server started successfully");
    }

}

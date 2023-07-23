package org.uom.distributed.systems.server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONObject;
import org.uom.distributed.systems.NodeManager;

public class MonitoringServer extends WebSocketServer {
    private static int client_count = 0;
    private static final HashMap<String, WebSocket> webSocketHashMap = new HashMap<>(2);
    private NodeManager nodeManager;
    public MonitoringServer(InetSocketAddress address) {
        super(address);
    }
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("connected");
    }
    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        assert nodeManager != null; nodeManager.killSystem();webSocketHashMap.clear();
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        JSONObject json = new JSONObject(message);
        webSocketHashMap.put((String) json.get("SOCKET"), conn);
        if(webSocketHashMap.containsKey("LOG") && webSocketHashMap.containsKey("COMMON")) {
            if (client_count <= 2) {
                nodeManager = new NodeManager(webSocketHashMap.get("COMMON"), webSocketHashMap.get("LOG"));

                int[][] input = {
                        {2, 3, 345}, {12, 45, 234}, {35, 45, 533}, {1, 38, 234}, {4, 5, 20}, {50, 3, 98}, {22, 2, 144}, {34, 33, 233}, {11, 39, 235}, {4, 1, 4000}, {4, 2, 4000}, {4, 3, 4000}, {3, 4, 4000}, {2, 4, 4000}, {1, 4, 4000}
                };
//            int[][] input = {
//                    {4, 1, 4000}, {4, 2, 4000}, {4, 3, 4000}, {3, 4, 4000}, {2, 4, 4000}, {1, 4, 4000}
//            };

                try {
                    nodeManager.initiateSystem(input);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                JSONObject e_message = new JSONObject().put("ERROR", "client already connected. Please restart the server and run the frontend.");
                conn.send(e_message.toString());
                conn.close();
            }
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

    public static void main(String[] args) throws InterruptedException {
        String host = "localhost";
        int port = 8887;

        WebSocketServer server = new MonitoringServer(new InetSocketAddress(host, port));
        server.run();

    }
}

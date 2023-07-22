package org.uom.distributed.systems;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
public class SimpleServer extends WebSocketServer {

    public SimpleServer(InetSocketAddress address) {
        super(address);
    }
    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        NodeManager nodeManager = new NodeManager(this);

        int[][] input = {
                {2,3,345},{12,45,234},{35,45,533},{1,38,234},{4,5,20},{50,3,98},{22,2,144},{34,33,233},{11,39,235},{4,1,4000},{4,2,4000},{4,3,4000},{3,4,4000},{2,4,4000},{1,4,4000}
        };
//        int[][] input = {
//                {4,1,4000},{4,2,4000},{4,3,4000},{3,4,4000},{2,4,4000},{1,4,4000}
//        };

        try {
            NodeManager.initiateSystem(input);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("closed " + conn.getRemoteSocketAddress() + " with exit code " + code + " additional info: " + reason);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("received message from " + conn.getRemoteSocketAddress() + ": " + message);
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

        WebSocketServer server = new SimpleServer(new InetSocketAddress(host, port));
        server.run();

    }
}

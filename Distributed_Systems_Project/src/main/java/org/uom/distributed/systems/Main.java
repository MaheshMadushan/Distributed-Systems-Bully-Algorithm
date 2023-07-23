package org.uom.distributed.systems;


import org.java_websocket.server.WebSocketServer;
import org.uom.distributed.systems.server.MonitoringServer;

import java.net.InetSocketAddress;


public class Main {

    private final static String host = "localhost";
    private final static int port = 8887;
    private final static WebSocketServer server = new MonitoringServer(new InetSocketAddress(host, port));
    public static void main(String[] args) throws InterruptedException {

        Thread serverThread = new Thread(server::run);
        serverThread.start();

        Thread.sleep(10000);
    }
}
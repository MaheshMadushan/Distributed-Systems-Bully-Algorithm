package org.uom.distributed.systems.Utilities;

import org.java_websocket.WebSocket;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogInterceptor {
    private final Logger logger;
    private final WebSocket conn;
    private final JSONObject logMessage;

    public LogInterceptor(Class _class, WebSocket log_con) {
        this.logger = LoggerFactory.getLogger(_class);
        this.conn = log_con;
        this.logMessage = new JSONObject().put("MESSAGE_TYPE", "LOG");
    }

    public void info(String s) {
        logger.info(s);
        logMessage.put("LOG_MESSAGE", s);
        conn.send(logMessage.toString());
    }
}

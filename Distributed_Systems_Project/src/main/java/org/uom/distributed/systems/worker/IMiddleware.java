package org.uom.distributed.systems.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.worker.middleware.MiddlewareType;

public interface IMiddleware {

    MiddlewareType getMiddlewareType();

    void handle(Message message) throws InterruptedException;

    void receiveMessage(Message message);
    void sendMessage(String recipientAddress, Message message);
    void stopProcess();

    void startProcess();
}

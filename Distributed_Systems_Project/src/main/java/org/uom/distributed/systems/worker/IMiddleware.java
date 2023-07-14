package org.uom.distributed.systems.worker;

import org.uom.distributed.systems.messaging.Message;
import org.uom.distributed.systems.worker.middleware.MiddlewareType;

public interface IMiddleware extends Runnable {
    MiddlewareType getMiddlewareType();

    void handle(Message message);
}

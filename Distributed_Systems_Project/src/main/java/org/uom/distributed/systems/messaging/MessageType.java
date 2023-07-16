package org.uom.distributed.systems.messaging;

public enum MessageType {
    TASK,
    ELECTION,
    OK,
    COORDINATOR,
    ASSIGN,
    MISC,
    ADD_FOLLOWER
}

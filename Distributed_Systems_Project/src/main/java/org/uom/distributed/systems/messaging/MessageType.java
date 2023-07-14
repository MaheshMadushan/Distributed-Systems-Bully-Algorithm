package org.uom.distributed.systems.messaging;

public enum MessageType {
    TASK,
    ELECTION,
    OK,
    COORDINATOR,
    ASSIGN,
    ADD_FOLLOWER
}

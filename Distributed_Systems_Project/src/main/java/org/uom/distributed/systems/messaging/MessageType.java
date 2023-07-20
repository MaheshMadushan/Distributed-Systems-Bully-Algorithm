package org.uom.distributed.systems.messaging;

public enum MessageType {
    TASK,
    ELECTION,
    OK,
    COORDINATOR,
    ASSIGN,
    INTERRUPT,
    MISC,
    BEACON,
    ADD_FOLLOWER,
    ADD_GROUP_MEMBER,
}

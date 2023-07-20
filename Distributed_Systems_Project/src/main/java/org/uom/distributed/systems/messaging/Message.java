package org.uom.distributed.systems.messaging;

import java.util.HashMap;

public class Message {
    private final MessageType messageType;
    private final HashMap<String, String> fields;
    private final String recipient;

    public Message(MessageType messageType, String recipientOrGroupID) {
        this.messageType = messageType;
        this.recipient = recipientOrGroupID;
        this.fields = new HashMap<>(10);
    }
    public MessageType getType() {
        return messageType;
    }

    public HashMap<String,String> getFields() {
        return fields;
    }

    public String getRecipientOrGroupID() {
        return recipient;
    }

    public Message addField(String key, String value) {
        fields.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        return "Message{" +
                "messageType=" + messageType +
                ", fields=" + fields.entrySet() +
                ", recipient='" + recipient + '\'' +
                '}';
    }
}

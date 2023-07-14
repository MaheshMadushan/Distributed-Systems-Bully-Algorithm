package org.uom.distributed.systems.messaging;

import java.util.HashMap;

public class Message {
    private final MessageType messageType;
    private final HashMap<String, String> fields;

    public Message(MessageType messageType) {
        this.messageType = messageType;
        this.fields = new HashMap<>(10);
    }
    public MessageType getType() {
        return messageType;
    }

    public HashMap<String,String> getFields() {
        return fields;
    }

    public Message addField(String key, String value) {
        fields.put(key, value);
        return this;
    }
}

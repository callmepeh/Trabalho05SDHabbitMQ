package com.distribuidos;

import java.time.Instant;
import java.util.UUID;

public class Message {
    private String id;
    private String timestamp;
    private String type;
    private String content;

    public Message() {}

    public Message(String type, String content) {
        this.id = UUID.randomUUID().toString();
        this.timestamp = Instant.now().toString();
        this.type = type;
        this.content = content;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
}

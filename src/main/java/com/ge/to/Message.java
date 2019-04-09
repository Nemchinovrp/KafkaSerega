package com.ge.to;

public class Message {
    private int priority;
    private String content;

    public Message(int priority, String content) {
        this.priority = priority;
        this.content = content;
    }

    public int getPriority() {
        return priority;
    }

    public String getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Message message = (Message) o;
        return getPriority() == message.getPriority()
                && (getContent() != null ? getContent().equals(message.getContent()) : message.getContent() == null);
    }

    @Override
    public int hashCode() {
        int result = getPriority();
        result = 31 * result + (getContent() != null ? getContent().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "priority=" + priority +
                ", content='" + content + '\'' +
                '}';
    }
}
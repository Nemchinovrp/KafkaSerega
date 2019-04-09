package com.ge;

import com.ge.to.Message;
import com.ge.to.SubscriberChannel;

public class Subscriber implements Runnable {
    private SubscriberChannel topic;
    private String name;

    public Subscriber(String name) {
        this.name = name;
    }

    public void run() {
        while (true) {
            if (topic != null && !topic.isEmpty()) {
                try {
                    Message message = topic.get();
                    System.out.println("Consumer: " + this.toString() + " " + message.getContent() + " Thread name " + Thread.currentThread().getName());
                    Thread.sleep(1000L);
                    topic.acknowledge();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void subscribeOnTopic(SubscriberChannel topic) {
        this.topic = topic;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Subscriber{" +
                "name='" + name + '\'' +
                '}';
    }
}
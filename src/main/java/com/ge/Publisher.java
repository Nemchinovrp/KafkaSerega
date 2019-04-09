package com.ge;

import com.ge.to.Message;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

public class Publisher implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
    private static final int HIGH_PRIORITY = 1;
    private static final int LOW_PRIORITY = 3;
    private final BlockingQueue<Message> topic;

    public Publisher(BlockingQueue<Message> topic) {
        this.topic = topic;
    }

    public void run() {
        for (Integer i = 0; i < 5; i++) {
            try {
                String content = RandomStringUtils.randomAlphabetic(10);
                Random random = new Random();
                int priority = random.ints(HIGH_PRIORITY, (LOW_PRIORITY + 1)).findFirst().getAsInt();
                topic.put(new Message(priority, content));
                logger.info("Produced message: content = {}, priority = {}", content, priority);
            } catch (InterruptedException e) {
                logger.error("Publisher error: ", e);
            }
        }
    }
}
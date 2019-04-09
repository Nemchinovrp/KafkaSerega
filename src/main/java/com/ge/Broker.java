package com.ge;

import com.ge.to.Message;
import com.ge.to.SubscriberChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * <code>Broker</code> class is a manager between a publisher and subscribers. The broker contains a <code>map</code> of
 * subscriber channel, journal.
 *
 * @see Journal
 * @see SubscriberChannel
 */
public class Broker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Broker.class);
    private Journal journal;
    private PriorityBlockingQueue<Message> messagesOfTopic = new PriorityBlockingQueue<>(100,
            Comparator.comparing(Message::getPriority));
    private Map<String, SubscriberChannel> subscribers = new ConcurrentHashMap<>();

    public Broker() {
        journal = new Journal();
    }

    public PriorityBlockingQueue<Message> getTopic() {
        return messagesOfTopic;
    }

    public void addSubscriber(Subscriber subscriber) {
        SubscriberChannel subscriberChannel = new SubscriberChannel(journal, subscriber);
        subscribers.put(subscriber.getName(), subscriberChannel);
        logger.info("Add new subscriber - {}", subscriber.getName());
    }

    @Override
    public void run() {
        try {
            resendMessages();
            do {
                List<Message> messages = new ArrayList<>();
                messagesOfTopic.drainTo(messages);
                for (SubscriberChannel blockingQueue : subscribers.values()) {
                    blockingQueue.addAll(messages);
                }
            } while (true);
        } finally {
            try {
                journal.close();
            } catch (IOException e) {
                logger.error("Journal cannot be closed: ", e);
            }
        }
    }

    //Resend messages after broker  recovery to subscribers from broker journal
    private void resendMessages() {
        Map<String, Set<Message>> unsentMessagesBySubscribers = journal.getUnsentMessages();
        if (!unsentMessagesBySubscribers.isEmpty()) {
            for (Map.Entry<String, Set<Message>> subscriber : unsentMessagesBySubscribers.entrySet()) {
                SubscriberChannel subscriberChannel = subscribers.get(subscriber.getKey());
                if (subscriberChannel != null) {
                    subscriberChannel.addAll(subscriber.getValue());
                } else {
                    logger.warn("Subscriber - '{}' is not subscribed to the topic", subscriber.getKey());
                }
            }
        }
    }
}
package com.ge.to;

import com.ge.Journal;
import com.ge.Subscriber;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * <code>SubscriberChannel</code> class provides connection between messages of topic and subscriber.
 * Object of this class contains a queue which are used by subscriber.
 *
 * @see SubscriberChannel#get
 * @see SubscriberChannel#acknowledge
 */
public class SubscriberChannel {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SubscriberChannel.class);
    private PriorityBlockingQueue<Message> messages = new PriorityBlockingQueue<>(100, Comparator.comparing(Message::getPriority));
    private Journal journal;
    private Subscriber subscriber;

    public SubscriberChannel(Journal journal, Subscriber subscriber) {
        this.journal = journal;
        this.subscriber = subscriber;
    }

    public void addAll(Collection<Message> messages) {
        this.messages.addAll(messages);
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    /**
     * Get a message from queue.
     *
     * @return message
     * @see Message
     */
    public Message get() {
        synchronized (this) {
            Message message = messages.peek();
            if (message != null) {
                journal.write(subscriber, message);
                logger.info("Subscriber: {}. Message was received: {}", subscriber.getName(), message);
            }
            return message;
        }
    }

    /**
     * This method confirms that the message has been processed and can be deleted.
     */
    public void acknowledge() {
        synchronized (this) {
            Message message = messages.remove();
            if (message != null) {
                journal.write(subscriber, message);
                logger.info("Subscriber: {}. Message ({}) can be deleted from subscriber channel.",
                        subscriber.getName(), message);
            }
        }
    }
}
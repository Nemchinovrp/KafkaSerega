package com.ge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Application {
    public static void main(String[] args) {
        Broker broker = new Broker();

        Publisher publisher = new Publisher(broker.getTopic());

        Subscriber subscriberOne = new Subscriber("System B1");
        Subscriber subscriberTwo = new Subscriber("System B2");

        broker.addSubscriber(subscriberOne);
        broker.addSubscriber(subscriberTwo);

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(broker);
        executorService.execute(publisher);
        executorService.execute(subscriberOne);
        executorService.execute(subscriberTwo);
    }
}

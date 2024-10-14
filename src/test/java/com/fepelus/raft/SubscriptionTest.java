package com.fepelus.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

/* How do you use JeroMQ? */
class SubscriptionTest {

    ZContext ctx;

    @BeforeEach
    void beforeEach() throws InterruptedException {
        ctx = new ZContext();
    }

    @Test
    void testPubsub() {
        Thread publishThread = publish();

        ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB);
        subscriber.connect("tcp://localhost:13697");
        subscriber.subscribe("one");
        String topic = subscriber.recvStr();
        assertEquals("one", topic);
        String body = subscriber.recvStr();
        assertEquals("body", body);

        publishThread.interrupt();
    }

    private Thread publish() {
        Thread output = new Thread(() -> {
            try {
                ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB);
                publisher.bind("tcp://*:13697");

                Thread.sleep(1000);

                publisher.sendMore("one");
                publisher.send("body");
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });
        output.start();
        return output;
    }

    @Test
    void testPubSubWithPolling() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        List<String> receivedMessages = new ArrayList<>();

        Thread publisherThread = publish(latch);
        Thread subscriberThread = subscribe(latch, receivedMessages);

        publisherThread.start();
        subscriberThread.start();

        assertTrue(
            latch.await(5, TimeUnit.SECONDS),
            "Timed out waiting for messages"
        );

        assertEquals(2, receivedMessages.size());
        assertEquals("one body", receivedMessages.get(0));
        assertEquals("two another-body", receivedMessages.get(1));

        publisherThread.interrupt();
        subscriberThread.interrupt();
    }

    private Thread publish(CountDownLatch latch) {
        return new Thread(() -> {
            try (ZMQ.Socket publisher = ctx.createSocket(SocketType.PUB)) {
                publisher.bind("tcp://*:13698");

                Thread.sleep(1000); // Give time for subscriber to connect

                publisher.sendMore("one");
                publisher.send("body");

                Thread.sleep(500);

                publisher.sendMore("two");
                publisher.send("another-body");

                latch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private Thread subscribe(
        CountDownLatch latch,
        List<String> receivedMessages
    ) {
        return new Thread(() -> {
            try (ZMQ.Socket subscriber = ctx.createSocket(SocketType.SUB)) {
                subscriber.connect("tcp://localhost:13698");
                subscriber.subscribe("".getBytes(ZMQ.CHARSET));

                Poller poller = ctx.createPoller(1);
                poller.register(subscriber, Poller.POLLIN);

                while (!Thread.currentThread().isInterrupted()) {
                    int events = poller.poll(1000);
                    if (events == -1) break; // Interrupted

                    if (poller.pollin(0)) {
                        String topic = subscriber.recvStr();
                        String body = subscriber.recvStr();
                        receivedMessages.add(topic + " " + body);

                        if (receivedMessages.size() == 2) {
                            latch.countDown();
                            break;
                        }
                    }
                }
            }
        });
    }
}

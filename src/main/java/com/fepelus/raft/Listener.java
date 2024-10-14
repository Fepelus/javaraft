package com.fepelus.raft;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Queue;
import java.util.stream.IntStream;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;

public class Listener implements StartOrStop {

    ZContext ctx;
    List<ZMQ.Socket> peerSubscription;
    Node thisNode;
    List<? extends Node> otherNodes;
    Queue<Event> queue;
    private final Deserialiser deserialiser;
    private Thread thread;

    public Listener(
        ZContext ctx,
        Node thisNode,
        List<? extends Node> otherNodes,
        Queue<Event> queue,
        Deserialiser deserialiser
    ) {
        this.ctx = ctx;
        this.thisNode = thisNode;
        this.otherNodes = otherNodes;
        this.queue = queue;
        this.deserialiser = deserialiser;
    }

    private void connect() {
        byte[] subscription = thisNode.name().getBytes(ZMQ.CHARSET);
        peerSubscription = otherNodes
            .stream()
            .map(node ->
                String.format("tcp://%s:%s", node.ip(), node.raftPort())
            )
            .map(address -> {
                Debug.log(thisNode, "Subscribed to " + address);
                return address;
            })
            .map(address -> {
                var socket = ctx.createSocket(SocketType.SUB);
                socket.connect(address);
                socket.subscribe(subscription);
                return socket;
            })
            .toList();
    }

    private void disconnect() {
        peerSubscription.forEach(ZMQ.Socket::close);
    }

    @Override
    public void start() {
        connect();
        thread = new Thread(this::loop);
        thread.start();
    }

    @Override
    public void stop() {
        thread.interrupt();
        disconnect();
    }

    public void loop() {
        while (!Thread.currentThread().isInterrupted()) {
            //  Initialize poll set
            Poller poller = ctx.createPoller(this.peerSubscription.size());
            this.peerSubscription
                .forEach(socket -> poller.register(socket, Poller.POLLIN));

            if (poller.poll(-1) < 0) break; //  Interrupted

            IntStream.range(0, this.peerSubscription.size())
                .filter(poller::pollin)
                .forEach(this::peerMessage);
        }
    }

    private void peerMessage(int index) {
        ZMQ.Socket socket = peerSubscription.get(index);
        Node peer = otherNodes.get(index);
        String givenName = socket.recvStr();

        assert givenName.equals(thisNode.name()) : String.format(
            "Names should have matched: %s != %s",
            givenName,
            thisNode.name()
        );

        Event event = deserialiser.deserialise(socket.recv());
        Debug.log(thisNode, String.format("Received message %s from %s", event.getClass().getSimpleName(), peer.name()));
        boolean success = queue.add(event);
        if (!success) {
            throw new RaftException("Could not add message to queue " + event);
        }
    }

}

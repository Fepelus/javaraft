package com.fepelus.raft;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.List;

public class Transport implements OutboundTransport, StartOrStop {

    ZContext ctx;
    ZMQ.Socket publisher;
    Node thisNode;
    List<? extends Node> otherNodes;
    private final Serialiser serialiser;

    public Transport(
        ZContext ctx,
        Node thisNode,
        List<? extends Node> otherNodes,
        Serialiser serialiser
    ) {
        this.ctx = ctx;
        this.thisNode = thisNode;
        this.otherNodes = otherNodes;
        this.serialiser = serialiser;
    }

    @Override
    public void start() {
        publisher = ctx.createSocket(SocketType.PUB);
        publisher.bind("tcp://*:" + thisNode.raftPort());
        Debug.log(thisNode, String.format("Publishing on tcp://*:%s", thisNode.raftPort()));
    }

    @Override
    public void stop() {
        publisher.close();
    }

    private void send(Event event, String toName) {
        Debug.log(thisNode, String.format("Sending %s to %s", event.getClass().getSimpleName(), toName));
        publisher.sendMore(toName);
        publisher.send(serialiser.serialise(event));
    }

    @Override
    public void sendLogResponse(SendLogResponseCommand command) {
        Node sendTo = command.sendTo();
        LogResponseEvent event = command.toEvent();
        send(event, sendTo.name());
    }

    @Override
    public void sendLogRequest(SendLogRequestCommand command) {
        Node sendTo = command.sendTo();
        LogRequestEvent event = command.toEvent();
        send(event, sendTo.name());
    }

    @Override
    public void sendVoteResponse(SendVoteResponseCommand command) {
        Node sendTo = command.sendTo();
        VoteResponseEvent event = command.toEvent();
        send(event, sendTo.name());
    }

    @Override
    public void sendVoteRequest(SendVoteRequestCommand command) {
        Node sendTo = command.sendTo();
        VoteRequestedEvent event = command.toEvent();
        send(event, sendTo.name());
    }

    @Override
    public void sendBroadcastRequest(SendBroadcastRequestCommand command) {
        Node sendTo = command.sendTo();
        BroadcastRequestedEvent event = command.toEvent();
        send(event, sendTo.name());
    }
}

package com.fepelus.raft;

import java.io.*;

sealed interface Event
    extends Serializable
    permits
        ElectionTimeoutEvent,
        VoteRequestedEvent,
        VoteResponseEvent,
        BroadcastRequestedEvent,
        PingScheduledEvent,
        LogRequestEvent,
        LogResponseEvent {}

record ElectionTimeoutEvent() implements Event {}

record VoteRequestedEvent(Node cId, long cTerm, int cLogSize, long cLogTerm)
    implements Event {}

record VoteResponseEvent(Node voterId, long term, boolean granted)
    implements Event {}

record BroadcastRequestedEvent(String message) implements Event {}

record PingScheduledEvent() implements Event {}

record LogRequestEvent(
    Node leaderId,
    long term,
    int prefixLen,
    long prefixTerm,
    int leaderCommit,
    Log suffix
)
    implements Event { }

record LogResponseEvent(Node followerId, long term, int ack, boolean success)
    implements Event {}

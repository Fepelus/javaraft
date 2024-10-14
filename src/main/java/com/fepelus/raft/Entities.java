package com.fepelus.raft;

import java.io.Serializable;
import java.util.List;

enum Role {
    Follower,
    Candidate,
    Leader,
}

interface Node {
    String name();
    String ip();
    String raftPort();
    String clientPort();
}

record LogEntry(String message, long term) implements Serializable {
    private static final long serialVersionUID = -3305276997530613803L;
}

interface PersistedState {
    long currentTerm();

    void incrementTerm();

    void setTerm(long newTerm);

    Log log();
}

interface Log {
    int size();

    void add(LogEntry entry);

    List<LogEntry> suffixFrom(int index);

    LogEntry get(int index);

    void truncateToLength(int prefixlen);
}

interface CommandHandler {
    void handle(Command command);
}

interface OutboundTransport {
    void sendLogResponse(SendLogResponseCommand command);
    void sendLogRequest(SendLogRequestCommand command);
    void sendVoteResponse(SendVoteResponseCommand command);
    void sendVoteRequest(SendVoteRequestCommand command);
    void sendBroadcastRequest(SendBroadcastRequestCommand command);
}

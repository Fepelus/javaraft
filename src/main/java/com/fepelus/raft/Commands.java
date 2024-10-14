package com.fepelus.raft;

import java.io.Serializable;
import java.util.List;

sealed interface Command
    permits
        SendVoteRequestCommand,
        SendVoteResponseCommand,
        SendLogRequestCommand,
        StartElectionTimerCommand,
        CancelElectionTimerCommand,
        StartPingTimerCommand,
        CancelPingTimerCommand,
        SendLogResponseCommand,
        SendBroadcastRequestCommand,
        DeliverToApplication {}

record SendVoteRequestCommand(
    Node sendTo,
    Node requestFrom,
    long currentTerm,
    int logSize,
    long lastTerm
)
    implements Command {
    public VoteRequestedEvent toEvent() {
        return new VoteRequestedEvent(
            requestFrom,
            currentTerm,
            logSize,
            lastTerm
        );
    }
}

record SendVoteResponseCommand(
    Node sendTo,
    Node voteFrom,
    long currentTerm,
    boolean voteSuccess
)
    implements Command {
    public VoteResponseEvent toEvent() {
        return new VoteResponseEvent(voteFrom, currentTerm, voteSuccess);
    }
}

record SendLogRequestCommand(
    Node sendTo,
    Node leaderId,
    long term,
    int prefixLen,
    long prefixTerm,
    int leaderCommit,
    List<LogEntry> suffix
)
    implements Command {
    public LogRequestEvent toEvent() {
        return new LogRequestEvent(
            leaderId,
            term,
            prefixLen,
            prefixTerm,
            leaderCommit,
            new ListLog(suffix)
        );
    }
}

record StartElectionTimerCommand() implements Command {}

record CancelElectionTimerCommand() implements Command {}

record StartPingTimerCommand() implements Command {}

record CancelPingTimerCommand() implements Command {}

record SendLogResponseCommand(
    Node sendTo,
    Node follower,
    long term,
    int ack,
    boolean success
)
    implements Command {
    public LogResponseEvent toEvent() {
        return new LogResponseEvent(follower, term, ack, success);
    }
}

record SendBroadcastRequestCommand(Node sendTo, String message)
    implements Command {
    BroadcastRequestedEvent toEvent() {
        return new BroadcastRequestedEvent(message);
    }
}

record DeliverToApplication(String message) implements Command {}

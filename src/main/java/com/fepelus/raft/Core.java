package com.fepelus.raft;

import java.util.*;
import java.util.stream.IntStream;

public class Core {

    Node thisNode;
    List<? extends Node> allNodeIds;
    Role role;
    PersistedState state;
    CommandHandler commandHandler;
    Node votedFor;
    Node currentLeader;
    Set<Node> votesReceived;
    Map<Node, Integer> sentLength;
    Map<Node, Integer> ackedLength;
    int commitLength;

    public Core(
        Node nodeId,
        List<? extends Node> allNodeIds,
        PersistedState state,
        CommandHandler commandHandler
    ) {
        this.thisNode = nodeId;
        this.allNodeIds = allNodeIds;
        this.state = state;
        this.commandHandler = commandHandler;
        this.role = Role.Follower;
        this.votedFor = null;
        this.currentLeader = null;
        this.votesReceived = new HashSet<>();
        this.sentLength = new HashMap<>();
        this.ackedLength = new HashMap<>();
        this.commitLength = 0;
        commandHandler.handle(new StartElectionTimerCommand());
    }

    public static Core recoverFromCrash(
        Node nodeId,
        List<Node> allNodeIds,
        PersistedState state,
        CommandHandler handler
    ) {
        return new Core(nodeId, allNodeIds, state, handler);
    }

    long currentTerm() {
        return state.currentTerm();
    }

    Object votedFor() {
        return votedFor;
    }

    Role currentRole() {
        return role;
    }

    Node currentLeader() {
        return currentLeader;
    }

    Set<Node> votesReceived() {
        return Collections.unmodifiableSet(votesReceived);
    }

    public void handle(Event event) {
        Debug.log(thisNode, String.format("Handling event %s", event.getClass().getSimpleName()));
        switch (event) {
            case ElectionTimeoutEvent ete -> this.handleElectionTimeout();
            case VoteRequestedEvent vre -> this.handleVoteRequested(vre);
            case VoteResponseEvent vre -> this.handleVoteResponse(vre);
            case BroadcastRequestedEvent bre -> this.handleBroadcastRequested(bre);
            case PingScheduledEvent pse -> this.handlePingScheduled();
            case LogRequestEvent lre -> this.handleLogRequest(lre);
            case LogResponseEvent lre -> this.handleLogResponse(lre);
        }
    }

    private void handleElectionTimeout() {
        Debug.log(thisNode, "Election timeout");
        state.incrementTerm();
        role = Role.Candidate;
        votedFor = thisNode;
        votesReceived.add(thisNode);
        final int logSize = state.log().size();
        final long lastTerm = lastLogTerm();

        allNodeIds.forEach(node ->
            commandHandler.handle(
                new SendVoteRequestCommand(
                    node,
                        thisNode,
                    state.currentTerm(),
                    logSize,
                    lastTerm
                )
            )
        );

        commandHandler.handle(new StartElectionTimerCommand());
    }

    private void handleVoteRequested(VoteRequestedEvent vre) {
        if (isNewTerm(vre.cTerm())) {
            resetTerm(vre.cTerm());
        }

        long lastTerm = lastLogTerm();
        int logSize = state.log().size();

        var logOk =
            (vre.cLogTerm() > lastTerm) ||
            (vre.cLogTerm() == lastTerm && vre.cLogSize() >= logSize);

        Debug.log(thisNode, String.format("Handling VoteRequestedEvent. eventTerm=%d nodeTerm=%d logOk=%b votedFor=%s", vre.cTerm(), state.currentTerm(), logOk, votedFor));
        if (
            vre.cTerm() == state.currentTerm() &&
            logOk &&
            (votedFor == null || votedFor.equals(vre.cId()))
        ) {
            votedFor = vre.cId();
            commandHandler.handle(
                new SendVoteResponseCommand(
                    vre.cId(),
                        thisNode,
                    state.currentTerm(),
                    true
                )
            );
        } else {
            commandHandler.handle(
                new SendVoteResponseCommand(
                    vre.cId(),
                        thisNode,
                    state.currentTerm(),
                    false
                )
            );
        }
    }

    private void handleVoteResponse(VoteResponseEvent vrs) {
        Debug.log(thisNode, String.format("Handling VoteResponseEvent. role:%s. eventTerm=%d nodeTerm=%d granted=%b", role.name(), vrs.term(), state.currentTerm(), vrs.granted()));
        if (
            role.equals(Role.Candidate) &&
            vrs.term() == state.currentTerm() &&
            vrs.granted()
        ) {
            votesReceived.add(vrs.voterId());
            Debug.log(thisNode, String.format("is %d >= %d ?", votesReceived.size(), quorum()));
            if (votesReceived.size() >= quorum()) {
                Debug.log(thisNode, "I am leader");
                role = Role.Leader;
                currentLeader = thisNode;
                commandHandler.handle(new CancelElectionTimerCommand());
                commandHandler.handle(new StartPingTimerCommand());
                allNodeIds
                    .stream()
                    .filter(id -> !id.equals(thisNode))
                    .forEach(follower -> {
                        sentLength.put(follower, state.log().size());
                        ackedLength.put(follower, 0);
                        replicateLog(follower);
                    });
            }
        }
        if (isNewTerm(vrs.term())) {
            resetTerm(vrs.term());
        }
    }

    private void handleBroadcastRequested(BroadcastRequestedEvent bre) {
        if (role.equals(Role.Leader)) {
            Debug.log(thisNode, "Handling BroadcastRequest on leader");
            state.log().add(new LogEntry(bre.message(), state.currentTerm()));
            ackedLength.put(thisNode, state.log().size());
            allNodeIds
                .stream()
                .filter(id -> !id.equals(thisNode))
                .forEach(this::replicateLog);
        } else if (currentLeader != null) {
            Debug.log(thisNode, "Handling BroadcastRequest on follower");
            commandHandler.handle(
                new SendBroadcastRequestCommand(currentLeader, bre.message())
            );
        } else {
            Debug.log(thisNode, "Handling BroadcastRequest before a leader has been elected");
            commandHandler.handle(
                    new SendBroadcastRequestCommand(null, bre.message())
            );
        }
    }

    private void handlePingScheduled() {
        Debug.log(thisNode, "Ping timeout");

        if (role.equals(Role.Leader)) {
            allNodeIds
                .stream()
                .filter(nId -> !nId.equals(thisNode))
                .forEach(this::replicateLog);
        }
    }

    private void handleLogRequest(LogRequestEvent lre) {
        if (lre.term() > state.currentTerm()) {
            resetTerm(lre.term());
        }
        if (lre.term() == state.currentTerm()) {
            if (role.equals(Role.Leader)) {
                commandHandler.handle(new CancelPingTimerCommand());
            }
            role = Role.Follower;
            currentLeader = lre.leaderId();
        }
        var logOk =
            (lre.prefixLen() <= state.log().size()) &&
            (lre.prefixLen() == 0 ||
                termAtLog(lre.prefixLen() - 1) == lre.prefixTerm());

        Debug.log(thisNode, String.format("Handling LogRequestEvent  eventTerm:%d nodeTerm:%d logOk:%b", lre.term(), state.currentTerm(), logOk));
        if (lre.term() == state.currentTerm() && logOk) {
            appendEntries(lre.prefixLen(), lre.leaderCommit(), lre.suffix());
            var ack = lre.prefixLen() + lre.suffix().size();
            Debug.log(thisNode, String.format("Replying to LogRequestEvent  nodeTerm:%d ack:%d success:%b", state.currentTerm(), ack, true));
            commandHandler.handle(
                new SendLogResponseCommand(
                    lre.leaderId(),
                        thisNode,
                    state.currentTerm(),
                    ack,
                    true
                )
            );
        } else {
            Debug.log(thisNode, String.format("Replying to LogRequestEvent  nodeTerm:%d ack:%d success:%b", state.currentTerm(), 0, false));
            commandHandler.handle(
                new SendLogResponseCommand(
                    lre.leaderId(),
                        thisNode,
                    state.currentTerm(),
                    0,
                    false
                )
            );
        }
        commandHandler.handle(new StartElectionTimerCommand());
    }

    private void handleLogResponse(LogResponseEvent lre) {

        if (lre.term() == state.currentTerm() && role.equals(Role.Leader)) {
        Debug.log(thisNode, String.format("Handling LogResponseEvent followerAckedLength:%d eventAckedLength:%d", ackedLength.get(lre.followerId()), lre.ack()));
            if (
                lre.success() && ackedLength.get(lre.followerId()) <= lre.ack()
            ) {
                sentLength.put(lre.followerId(), lre.ack());
                ackedLength.put(lre.followerId(), lre.ack());
                commitLogEntries();
            } else if (0 < sentLength.get(lre.followerId())) {
                sentLength.put(
                    lre.followerId(),
                    sentLength.get(lre.followerId()) - 1
                );
                replicateLog(lre.followerId());
            }
            commandHandler.handle(new StartPingTimerCommand());
        } else if (isNewTerm(lre.term())) {
            resetTerm(lre.term());
        }
    }

    private void commitLogEntries() {

        List<Integer> ready = IntStream.rangeClosed(1, state.log().size())
                .filter(len -> quorum() <= acks(len))
                .boxed()
                .toList();
        Debug.log(thisNode, String.format("CommitLogEntries-a logsize:%d acks:[%d,%d] readyEmpty:%b", state.log().size(), acks(1), acks(2), ready.isEmpty()));
        if (ready.isEmpty()) {
            return;
        }
        int maxReady = Collections.max(ready);
        Debug.log(thisNode, String.format("CommitLogEntries-b commitLength:%d < maxReady:%d   termAtLog(%d):%d == currentTerm:%d", commitLength, maxReady, maxReady-1, termAtLog(maxReady-1), state.currentTerm()));
        if (
            commitLength < maxReady &&
            termAtLog(maxReady - 1) == state.currentTerm()
        ) {
            Debug.log(thisNode, "Delivering to application");

            for (int i = commitLength; i < maxReady; i++) {
                commandHandler.handle(
                    new DeliverToApplication(state.log().get(i).message())
                );
            }

            commitLength = maxReady;
        }
    }

    private void appendEntries(int prefixLen, int leaderCommit, Log suffix) {
        if (0 < suffix.size() && prefixLen < state.log().size()) {
            var index =
                Integer.min(state.log().size(), prefixLen + suffix.size()) - 1;
            if (termAtLog(index) != suffix.get(index - prefixLen).term()) {
            Debug.log(thisNode, String.format("Truncating log to length %d", prefixLen));
                state.log().truncateToLength(prefixLen);
            }
        }
        if (state.log().size() < (prefixLen + suffix.size())) {
            Debug.log(thisNode, "Adding suffix to log");

            for (
                int i = state.log().size() - prefixLen;
                i < suffix.size();
                i++
            ) {
                state.log().add(suffix.get(i));
            }
        }
        if (commitLength < leaderCommit) {
            Debug.log(thisNode, "Delivering to application");
            for (int i = commitLength; i < leaderCommit; i++) {
                commandHandler.handle(
                    new DeliverToApplication(state.log().get(i).message())
                );
            }
            commitLength = leaderCommit;
        }
    }

    private void replicateLog(Node follower) {
        int prefixLen = sentLength.get(follower);
        List<LogEntry> suffix = state.log().suffixFrom(prefixLen);
        long prefixTerm = 0;
        if (0 < prefixLen) {
            prefixTerm = termAtLog(prefixLen - 1);
        }
        commandHandler.handle(
            new SendLogRequestCommand(
                follower,
                    thisNode,
                state.currentTerm(),
                prefixLen,
                prefixTerm,
                commitLength,
                suffix
            )
        );
    }

    private long acks(int length) {
        return ackedLength
            .values()
            .stream()
            .filter(len -> length <= len)
            .count();
    }

    private boolean isNewTerm(long otherTerm) {
        return state.currentTerm() < otherTerm;
    }

    private void resetTerm(long newTerm) {
        if (role.equals(Role.Leader)) {
            commandHandler.handle(new CancelPingTimerCommand());
        }
        state.setTerm(newTerm);
        role = Role.Follower;
        votedFor = null;
        votesReceived.clear();
        commandHandler.handle(new CancelElectionTimerCommand());
    }

    private int quorum() {
        return (int) Math.ceil((allNodeIds.size() + 1) / 2.0f);
    }

    private long lastLogTerm() {
        int logSize = state.log().size();
        if (0 < logSize) {
            return termAtLog(logSize - 1);
        }
        return 0;
    }

    private long termAtLog(int i) {
        Debug.log(thisNode, String.format("termAtLog(%d) %s", i, state));
        LogEntry lastEntry = state.log().get(i);
        if (lastEntry == null) {
            return 0;
        }
        return lastEntry.term();
    }
}

package com.fepelus.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.*;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CoreTest {

    @Nested
    class Initialisation {

        Core core;
        TestHandler testHandler;

        @BeforeEach
        void beforeEach() {
            var nodeId = new TestNode();
            var state = new InMemoryState();
            testHandler = new TestHandler();
            core = new Core(
                nodeId,
                List.of(nodeId, new TestNode()),
                state,
                testHandler
            );
        }

        @Test
        void currentTerm() {
            assertEquals(0, core.currentTerm());
        }

        @Test
        void votedFor() {
            assertNull(core.votedFor());
        }

        @Test
        void currentRole() {
            assertEquals(Role.Follower, core.currentRole());
        }

        @Test
        void currentLeader() {
            assertNull(core.currentLeader());
        }

        @Test
        void votesReceived() {
            assertEquals(0, core.votesReceived().size());
        }

        @Test
        void commandsEmitted() {
            testHandler.expect(CommandList.of(new StartElectionTimerCommand()));
        }
    }

    @Nested
    class OnRecoveryFromCrash {

        Core core;

        @BeforeEach
        void beforeEach() {
            var nodeId = new TestNode();
            var state = new InMemoryState();
            CommandHandler testHandler = new TestHandler();
            core = Core.recoverFromCrash(
                nodeId,
                List.of(nodeId, new TestNode(), new TestNode()),
                state,
                testHandler
            );
        }

        @Test
        void currentRole() {
            assertEquals(Role.Follower, core.currentRole());
        }

        @Test
        void currentLeader() {
            assertNull(core.currentLeader());
        }

        @Test
        void votesReceived() {
            assertEquals(0, core.votesReceived().size());
        }
    }

    @Nested
    class OnElectionTimeout {

        Core core;
        Node nodeId;
        Node otherNodeId;
        TestHandler testHandler;

        @BeforeEach
        void beforeEach() {
            nodeId = new TestNode();
            otherNodeId = new TestNode();
            var state = new InMemoryState();
            testHandler = new TestHandler();
            core = new Core(
                nodeId,
                List.of(nodeId, otherNodeId),
                state,
                testHandler
            );
            core.handle(new ElectionTimeoutEvent());
        }

        @Test
        void currentTermIncremented() {
            assertEquals(1, core.currentTerm());
        }

        @Test
        void votedForSelf() {
            assertEquals(nodeId, core.votedFor());
        }

        @Test
        void currentRole() {
            assertEquals(Role.Candidate, core.currentRole());
        }

        @Test
        void votesReceived() {
            assertEquals(1, core.votesReceived().size());
            assertTrue(core.votesReceived().contains(nodeId));
        }

        @Test
        void commandsEmitted() {
            testHandler.expect(
                CommandList.of(
                    new StartElectionTimerCommand(),
                    new SendVoteRequestCommand(nodeId, nodeId, 1, 0, 0),
                    new SendVoteRequestCommand(otherNodeId, nodeId, 1, 0, 0),
                    new StartElectionTimerCommand()
                )
            );
        }
    }

    @Nested
    class OnVoteRequested {

        Core core;
        Node nodeId;
        Node cId;
        TestHandler testHandler;

        @BeforeEach
        void beforeEach() {
            nodeId = new TestNode();
            cId = new TestNode();
            var state = new InMemoryState();
            testHandler = new TestHandler();
            core = new Core(nodeId, List.of(nodeId, cId), state, testHandler);
        }

        @Nested
        class RequestNewTerm {

            long cTerm = 1;
            int cLogSize = 0;
            long cLogTerm = 0;

            @BeforeEach
            void beforeEach() {
                core.handle(
                    new VoteRequestedEvent(cId, cTerm, cLogSize, cLogTerm)
                );
            }

            @Test
            void currentTermIncremented() {
                assertEquals(1, core.currentTerm());
            }

            @Test
            void voted() {
                assertEquals(cId, core.votedFor());
            }

            @Test
            void commandsEmitted() {
                testHandler.expect(
                    CommandList.of(
                        new StartElectionTimerCommand(),
                        new CancelElectionTimerCommand(),
                        new SendVoteResponseCommand(cId, nodeId, 1, true)
                    )
                );
            }
        }

        @Nested
        class RequestSameTerm {

            long cTerm = 1;
            int cLogSize = 0;
            long cLogTerm = 1;

            @BeforeEach
            void beforeEach() {
                core.handle(new ElectionTimeoutEvent());
                core.handle(
                    new VoteRequestedEvent(cId, cTerm, cLogSize, cLogTerm)
                );
            }

            @Test
            void currentTermIncremented() {
                assertEquals(1, core.currentTerm());
            }

            @Test
            void voted() {
                assertEquals(nodeId, core.votedFor());
            }

            @Test
            void commandsEmitted() {
                testHandler.expect(
                    CommandList.of(
                        new StartElectionTimerCommand(),
                        new SendVoteRequestCommand(nodeId, nodeId, 1, 0, 0),
                        new SendVoteRequestCommand(cId, nodeId, 1, 0, 0),
                        new StartElectionTimerCommand(),
                        new SendVoteResponseCommand(cId, nodeId, 1, false)
                    )
                );
            }
        }
    }

    @Nested
    class OnVoteReceived {

        Core core;
        Node nodeId;
        Node vId;
        TestHandler testHandler;

        @BeforeEach
        void beforeEach() {
            nodeId = new TestNode();
            vId = new TestNode();
            var state = new InMemoryState();
            testHandler = new TestHandler();
            core = new Core(nodeId, List.of(nodeId, vId), state, testHandler);
            core.handle(new ElectionTimeoutEvent());
        }

        @Nested
        class GoodVote {

            long term = 1;

            @BeforeEach
            void beforeEach() {
                core.handle(new VoteResponseEvent(vId, term, true));
            }

            @Test
            void votesReceived() {
                assertEquals(2, core.votesReceived().size());
                assertTrue(core.votesReceived().contains(vId));
                assertTrue(core.votesReceived().contains(nodeId));
            }

            @Test
            void commandsEmitted() {
                testHandler.expect(
                    CommandList.of(
                        new StartElectionTimerCommand(),
                        new SendVoteRequestCommand(nodeId, nodeId, 1, 0, 0),
                        new SendVoteRequestCommand(vId, nodeId, 1, 0, 0),
                        new StartElectionTimerCommand(),
                        new CancelElectionTimerCommand(),
                        new StartPingTimerCommand(),
                        new SendLogRequestCommand(
                            vId,
                            nodeId,
                            1,
                            0,
                            0,
                            0,
                            List.of()
                        )
                    )
                );
            }
        }
    }

    @Nested
    class BroadcastMessages {

        Core core;
        Node nodeId;
        Node vId;
        TestHandler testHandler;
        PersistedState state;

        @BeforeEach
        void beforeEach() {
            nodeId = new TestNode();
            vId = new TestNode();
            state = new InMemoryState();
            testHandler = new TestHandler();
            core = new Core(nodeId, List.of(nodeId, vId), state, testHandler);
        }

        @Nested
        class WhenLeader {

            @BeforeEach
            void beforeEach() {
                core.handle(new ElectionTimeoutEvent());
                core.handle(new VoteResponseEvent(vId, 1, true));
            }

            @Test
            void appendedToLog() {
                core.handle(new BroadcastRequestedEvent("message"));
                assertEquals(1, state.log().size());
            }

            @Test
            void commandsEmitted() {
                core.handle(new BroadcastRequestedEvent("message"));
                testHandler.expect(
                    CommandList.of(
                        new StartElectionTimerCommand(),
                        new SendVoteRequestCommand(nodeId, nodeId, 1, 0, 0),
                        new SendVoteRequestCommand(vId, nodeId, 1, 0, 0),
                        new StartElectionTimerCommand(),
                        new CancelElectionTimerCommand(),
                        new StartPingTimerCommand(),
                        // Doesn't seem right: why send an empty log?
                        new SendLogRequestCommand(
                            vId,
                            nodeId,
                            1,
                            0,
                            0,
                            0,
                            List.of()
                        ),
                        new SendLogRequestCommand(
                            vId,
                            nodeId,
                            1,
                            0,
                            0,
                            0,
                            List.of(new LogEntry("message", 1))
                        )
                    )
                );
            }
        }

        @Nested
        class WhenFollower {

            @Test
            void commandsEmitted() {
                core.handle(
                    new LogRequestEvent(vId, 1L, 0, 0L, 0, new ListLog())
                );
                core.handle(new BroadcastRequestedEvent("message"));
                testHandler.expect(
                    CommandList.of(
                        new StartElectionTimerCommand(),
                        new CancelElectionTimerCommand(),
                        new SendLogResponseCommand(vId, nodeId, 1, 0, true),
                        new StartElectionTimerCommand(),
                        new SendBroadcastRequestCommand(vId, "message")
                    )
                );
            }
        }
    }

    @Nested
    class Ping {

        Core core;
        Node nodeId;
        Node vId;
        TestHandler testHandler;
        PersistedState state;

        @BeforeEach
        void beforeEach() {
            nodeId = new TestNode();
            vId = new TestNode();
            state = new InMemoryState();
            testHandler = new TestHandler();
            core = new Core(nodeId, List.of(nodeId, vId), state, testHandler);
        }

        @Nested
        class WhenLeader {

            @BeforeEach
            void beforeEach() {
                core.handle(new ElectionTimeoutEvent());
                core.handle(new VoteResponseEvent(vId, 1, true));
            }

            @Test
            void commandsEmitted() {
                core.handle(new PingScheduledEvent());
                testHandler.expect(
                    CommandList.of(
                        new StartElectionTimerCommand(),
                        new SendVoteRequestCommand(nodeId, nodeId, 1, 0, 0),
                        new SendVoteRequestCommand(vId, nodeId, 1, 0, 0),
                        new StartElectionTimerCommand(),
                        new CancelElectionTimerCommand(),
                        new StartPingTimerCommand(),
                        new SendLogRequestCommand(
                            vId,
                            nodeId,
                            1,
                            0,
                            0,
                            0,
                            List.of()
                        ),
                        new SendLogRequestCommand(
                            vId,
                            nodeId,
                            1,
                            0,
                            0,
                            0,
                            List.of()
                        )
                    )
                );
            }
        }

        @Nested
        class WhenFollower {

            @Test
            void commandsEmitted() {
                core.handle(new PingScheduledEvent());
                testHandler.expect(
                    CommandList.of(new StartElectionTimerCommand())
                );
            }
        }
    }

    @Nested
    class LogRequest {

        Core core;
        Node nodeId;
        Node lId;
        TestHandler testHandler;
        PersistedState state;

        @BeforeEach
        void beforeEach() {
            nodeId = new TestNode();
            lId = new TestNode();
            state = new InMemoryState();
            testHandler = new TestHandler();
            core = new Core(nodeId, List.of(nodeId, lId), state, testHandler);
        }

        @Test
        void newTermLaterThanCurrentTerm() {
            core.handle(new LogRequestEvent(lId, 2L, 0, 0L, 0, new ListLog()));
            assertEquals(2, core.currentTerm());
            assertNull(core.votedFor());
            assertEquals(lId, core.currentLeader());
            testHandler.expect(
                CommandList.of(
                    new StartElectionTimerCommand(),
                    new CancelElectionTimerCommand(),
                    new SendLogResponseCommand(lId, nodeId, 2, 0, true),
                    new StartElectionTimerCommand()
                )
            );
        }

        @Test
        void newTermEarlierThanCurrentTerm() {
            core.handle(new ElectionTimeoutEvent());
            core.handle(new LogRequestEvent(lId, 0L, 0, 0L, 0, new ListLog()));
            assertEquals(1, core.currentTerm());
            assertEquals(nodeId, core.votedFor());
            testHandler.expect(
                CommandList.of(
                    new StartElectionTimerCommand(),
                    new SendVoteRequestCommand(nodeId, nodeId, 1, 0, 0),
                    new SendVoteRequestCommand(lId, nodeId, 1, 0, 0),
                    new StartElectionTimerCommand(),
                    new SendLogResponseCommand(lId, nodeId, 1, 0, false),
                    new StartElectionTimerCommand()
                )
            );
        }
    }

    @Nested
    class LogResponse {

        @Test
        void onLogResponseEvent() {
            Node nodeId = new TestNode();
            Node fId = new TestNode();
            PersistedState state = new InMemoryState();
            TestHandler testHandler = new TestHandler();
            Core core = new Core(
                nodeId,
                List.of(nodeId, fId),
                state,
                testHandler
            );
            core.handle(new ElectionTimeoutEvent());
            core.handle(new VoteResponseEvent(fId, 1, true));
            core.handle(new LogResponseEvent(fId, 1, 0, true));
            testHandler.expect(
                CommandList.of(
                    new StartElectionTimerCommand(),
                    new SendVoteRequestCommand(nodeId, nodeId, 1, 0, 0),
                    new SendVoteRequestCommand(fId, nodeId, 1, 0, 0),
                    new StartElectionTimerCommand(),
                    new CancelElectionTimerCommand(),
                    new StartPingTimerCommand(),
                    new SendLogRequestCommand(
                        fId,
                        nodeId,
                        1,
                        0,
                        0,
                        0,
                        List.of()
                    ),
                    new StartPingTimerCommand()
                )
            );
        }
    }
}

class InMemoryState implements PersistedState {

    long currentTerm = 0;
    Log log = new ListLog();

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public void incrementTerm() {
        currentTerm += 1;
    }

    @Override
    public void setTerm(long newTerm) {
        currentTerm = newTerm;
    }

    @Override
    public Log log() {
        return log;
    }
}


class CommandList {

    List<Command> list;

    CommandList() {
        this.list = new ArrayList<>();
    }

    static CommandList of(Command... input) {
        var output = new CommandList();
        output.list = Arrays.asList(input);
        return output;
    }

    void add(Command command) {
        this.list.add(command);
    }

    Command get(int index) {
        return this.list.get(index-1);
    }

    int size() {
        return this.list.size();
    }

    boolean matches(CommandList other) {
        if (size() != other.size()) {
            return false;
        }
        for (int i = 1; i <= size(); i++) {
            if (!get(i).equals(other.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return (
            "[" +
            this.list.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ")) +
            "]"
        );
    }
}

class TestHandler implements CommandHandler {

    CommandList actualCommands = new CommandList();

    @Override
    public void handle(Command command) {
        actualCommands.add(command);
    }

    void expect(CommandList expectedCommands) {
        if (!expectedCommands.matches(actualCommands)) {
            throw new AssertionError(
                "\n--- Expected:\n" +
                expectedCommands.toString() +
                "\n--- but was:\n" +
                actualCommands.toString()
            );
        }
    }
}

class TestNode implements Node {

    @Override
    public String ip() {
        return "127.0.0.1";
    }

    @Override
    public String raftPort() {
        return "16331";
    }

    @Override
    public String clientPort() {
        return "16330";
    }

    @Override
    public String name() {
        return "test node";
    }
}

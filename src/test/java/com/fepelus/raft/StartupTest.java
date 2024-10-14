package com.fepelus.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

class StartupTest {

    List<Thread> threads = new ArrayList<>();
    RaftClient client;

    @BeforeEach
    void beforeEach() throws InterruptedException {
        client = new RaftClient();
        threads.add(startAlice());
        threads.add(startBob());
        threads.add(startCharlie());
        Thread.sleep(5000);
    }

    @AfterEach
    void afterEach() {
        threads.forEach(Thread::interrupt);
    }

    @Test
    void sendACommand() {
        String reply = client.send("set a 1");
        assertEquals("1", reply);
        reply = client.send("set b 2");
        assertEquals("2", reply);
        reply = client.send("get a");
        assertEquals("1", reply);
        reply = client.send("get b");
        assertEquals("2", reply);
    }

    Thread startAlice() {
        return startNodeWithConfig("/alice.config.kdl");
    }

    Thread startBob() {
        return startNodeWithConfig("/bob.config.kdl");
    }

    Thread startCharlie() {
        return startNodeWithConfig("/charlie.config.kdl");
    }

    private Thread startNodeWithConfig(String resourceName) {
        Thread thread = Thread.ofPlatform()
            .start(() -> {
                try {
                    Config config = getConfigFromResource(resourceName);
                    StartOrStop app = Server.compositionRoot(config);
                    app.start();
                } catch (RaftException e) {
                    System.err.printf(
                        "Raft exception starting %s: %s",
                        resourceName,
                        e.getMessage()
                    );
                } catch (IOException e) {
                    System.err.printf(
                        "IOException starting %s: %s",
                        resourceName,
                        e.getMessage()
                    );
                }
            });
        return thread;
    }

    private Config getConfigFromResource(String resourceName)
        throws IOException {
        InputStream stream = KdlTest.class.getResourceAsStream(resourceName);
        return new KdlConfig(stream);
    }
}

class RaftClient {

    private final ZContext ctx;

    RaftClient() {
        this.ctx = new ZContext();
    }

    public String send(String string) {
        //  Socket to talk to server
        ZMQ.Socket request = ctx.createSocket(SocketType.REQ);
        request.connect("tcp://127.0.0.1:5551");

        request.send(string);
        String message = request.recvStr();
        request.close();
        return message;
    }
}

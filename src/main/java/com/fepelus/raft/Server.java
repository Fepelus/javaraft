package com.fepelus.raft;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.zeromq.ZContext;

public class Server {

    public static void main(String[] args) {
        try {
            Config config = loadConfig(args[0]);
            StartOrStop app = compositionRoot(config);
            app.start();
        } catch (RaftException e) {
            System.err.println(e);
            System.exit(1);
        }
    }

    static Config loadConfig(String filename) {
        try {
            InputStream is = new FileInputStream(filename);
            return new KdlConfig(is);
        } catch (FileNotFoundException fnfe) {
            throw new RaftException("Could not open " + filename, fnfe);
        } catch (IOException ioe) {
            throw new RaftException("Could not parse " + filename, ioe);
        }
    }

    static StartOrStop compositionRoot(Config config) throws RaftException {
        SQLiteState state = new SQLiteState(config);
        ZContext ctx = new ZContext();
        Serialisation serialisation = new Serialisation();
        Transport transport = new Transport(
            ctx,
            config.thisNode(),
            config.otherNodes(),
                serialisation
        );
        Queue<Event> queue = new ConcurrentLinkedQueue<>();
        MemoryTimer timer = new MemoryTimer(queue, config.timeout());
        timer.onPingTimeout(() -> queue.add(new PingScheduledEvent()));
        timer.onElectionTimeout(() -> queue.add(new ElectionTimeoutEvent()));
        Listener listener = new Listener(
            ctx,
            config.thisNode(),
            config.otherNodes(),
            queue,
                serialisation
        );
        Application application = new Application(
            ctx,
            config.thisNode(),
            queue
        );
        Handler handler = new Handler(config, timer, transport, application);
        Core core = new Core(
            config.thisNode(),
            config.allNodes(),
            state,
            handler
        );
        Consensus consensus = new Consensus(
            config,
            state,
            handler,
            transport,
            queue,
            core
        );
        return new NeedStarting(application, listener, transport, consensus);
    }
}

class RaftException extends RuntimeException {

    RaftException(String message) {
        super(message);
    }

    RaftException(String message, Exception wrapped) {
        super(message, wrapped);
    }
}

interface StartOrStop {
    void start();
    void stop();
}

class NeedStarting implements StartOrStop {

    StartOrStop[] needStarting;

    NeedStarting(StartOrStop... needStarting) {
        this.needStarting = needStarting;
    }

    @Override
    public void start() {
        Arrays.asList(needStarting).stream().forEach(StartOrStop::start);
    }

    @Override
    public void stop() {
        Arrays.asList(needStarting).stream().forEach(StartOrStop::stop);
    }
}

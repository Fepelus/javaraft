package com.fepelus.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Application implements ConsensusListener, StartOrStop {

    ZContext ctx;
    ZMQ.Socket socket;
    Node thisNode;
    Queue<Event> queue;
    private Thread thread;
    private Map<String, String> applicationMap;
    private boolean replyNeeded;

    public Application(ZContext ctx, Node thisNode, Queue<Event> queue) {
        this.ctx = ctx;
        this.thisNode = thisNode;
        this.queue = queue;
        this.applicationMap = new HashMap<>();
        this.replyNeeded = false;
    }

    private void connect() {
        socket = ctx.createSocket(SocketType.REP);
        socket.bind(String.format("tcp://*:%s", thisNode.clientPort()));
    }

    private void disconnect() {
        socket.close();
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
            String msg = socket.recvStr();
            Debug.log(thisNode, String.format("Received: [%s]", msg));

            switch (Parsed.parse(msg)) {
                case SetCommand setCommand -> {
                    queue.add(new BroadcastRequestedEvent(msg));
                    replyNeeded = true;
                }
                case GetCommand getCommand ->
                    socket.send(Optional.ofNullable(applicationMap.get(getCommand.key())).orElse("<not found>"));
                case NotParsed notParsed -> socket.send(String.format("Could not parse: '%s'%n", notParsed.unParsedCommand));
                default -> System.err.printf("Could not parse: %s\n", msg);
            }
        }
    }

    @Override
    public void onDelivery(DeliverToApplication command) {
        Debug.log(thisNode, String.format("Deliver to application: %s", command));
        if (Parsed.parse(command.message()) instanceof SetCommand setCommand) {
            applicationMap.put(setCommand.key(), setCommand.value());
            if (replyNeeded) {
                socket.send(applicationMap.get(setCommand.key()));
            }
        }
        if (replyNeeded) {
            replyNeeded = false;
        }
    }

    @Override
    public void sendBroadcastRequest(SendBroadcastRequestCommand command) {
        if (command.sendTo() == null) {
            socket.send("retry: no leader");
        } else {
            socket.send("retry: " + command.sendTo().name());
        }

        if (replyNeeded) {
            replyNeeded = false;
        }
    }

}

interface Parsed {
    @SuppressWarnings("StringSplitter")
    static Parsed parse(String input) {
        String[] split = input.split(" ");
        if (2 < split.length && split[0].equals("set")) {
            return new SetCommand(split[1], split[2]);
        }
        if (1 < split.length && split[0].equals("get")) {
            return new GetCommand(split[1]);
        }
        return new NotParsed(input);
    }
}

class SetCommand implements Parsed {

    private String key;
    private String value;

    public SetCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }
}

class GetCommand implements Parsed {

    private String key;

    public GetCommand(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}

class NotParsed implements Parsed {

    String unParsedCommand;

    public NotParsed(String unParsedCommand) {
        this.unParsedCommand = unParsedCommand;
    }
}

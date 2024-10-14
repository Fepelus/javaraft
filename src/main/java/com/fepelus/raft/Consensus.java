package com.fepelus.raft;

import java.util.Queue;

/*
Start a thread that loops over each connection to the read port:
      https://stackoverflow.com/questions/20981455/how-do-i-send-a-message-over-a-network
Each message read gets put on input message ConcurrentQueue

Main thread loops over each entry on the input message queue
   and calls core.handle()

Do not need an extra thread on the output side.

Each node has two ports: the peer port for Raft messages between nodes
  and the req rep port for clients
*/

class Consensus implements StartOrStop {

    Config config;
    SQLiteState state;
    Handler handler;
    Transport transport;
    Queue<Event> queue;
    Core core;
    private Thread thread;

    public Consensus(
        Config config,
        SQLiteState state,
        Handler handler,
        Transport transport,
        Queue<Event> queue,
        Core core
    ) {
        this.config = config;
        this.state = state;
        this.handler = handler;
        this.transport = transport;
        this.queue = queue;
        this.core = core;
    }

    void sendTo() {}

    @Override
    public void start() {
        thread = new Thread(this::loop);
        thread.start();
    }

    private void loop() {
        while (!Thread.currentThread().isInterrupted()) {
            if (queue.peek() != null) {
                Debug.log(config.thisNode(), String.format("Pulled message off queue: %s", queue.peek().getClass().getSimpleName()));
                core.handle(queue.poll());
            }
        }
    }

    @Override
    public void stop() {
        thread.interrupt();
    }
}

package com.fepelus.raft;

import java.util.*;

interface Timer {
    void startElectionTimer();

    void cancelElectionTimer();

    void startPingTimer();

    void cancelPingTimer();
}

interface TimeWatcher {
    void onPingTimeout(Runnable callback);
    void onElectionTimeout(Runnable callback);
}

class MemoryTimer implements Timer, TimeWatcher {

    List<Runnable> pingCallbacks = new ArrayList<>();
    List<Runnable> electionCallbacks = new ArrayList<>();
    int electionTimeout;
    java.util.Timer electionTimer;
    java.util.Timer pingTimer;
    Random random;
    Queue<Event> queue;

    public MemoryTimer(Queue<Event> queue, int timeout) {
        this.queue = queue;
        electionTimeout = timeout;
        random = new Random();
    }

    @Override
    public void startElectionTimer() {
        if (electionTimer != null) {
            cancelElectionTimer();
        }
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                electionCallbacks.forEach(Runnable::run);
            }
        };
        electionTimer = new java.util.Timer("Timer");

        long delay = electionTimeout;
        electionTimer.schedule(task, randomise(delay));
    }

    private long randomise(long target) {
        long min = 95L * target;
        long max = 105L * target;
        long randomed = random.nextLong(max - min) + min;
        return randomed / 100L;
    }

    @Override
    public void cancelElectionTimer() {
        electionTimer.cancel();
    }

    @Override
    public void startPingTimer() {
        if (pingTimer != null) {
            cancelPingTimer();
        }
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                pingCallbacks.forEach(Runnable::run);
            }
        };
        pingTimer = new java.util.Timer("Timer");

        long delay = electionTimeout / 5L;
        pingTimer.schedule(task, delay, delay);
    }

    @Override
    public void cancelPingTimer() {
        pingTimer.cancel();
    }

    @Override
    public void onPingTimeout(Runnable callback) {
        pingCallbacks.add(callback);
    }

    @Override
    public void onElectionTimeout(Runnable callback) {
        electionCallbacks.add(callback);
    }
}

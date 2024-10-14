package com.fepelus.raft;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class ListLog implements Serializable, Log {
    @Serial
    private static final long serialVersionUID = -3305276997530L;

    private List<LogEntry> log;

    ListLog() {
        this(new ArrayList<>());
    }

    ListLog(List<LogEntry> log) {
        this.log = log;
    }

    /*
    private void writeObject(ObjectOutputStream out) throws IOException {
        System.out.println("WRITING OBJECT");
        out.defaultWriteObject();

        // Write the size of the log
        out.writeInt(log.size());

        // Write each LogEntry
        for (LogEntry entry : log) {
            out.writeObject(entry.message());
            out.writeLong(entry.term());
        }
    }

    private void readObject(ObjectInputStream in)
        throws IOException, ClassNotFoundException {
        System.out.println("READING OBJECT");
        in.defaultReadObject();

        // Read the size of the log
        int size = in.readInt();

        // Initialize the log
        log = new ArrayList<>(size);

        // Read each LogEntry
        for (int i = 0; i < size; i++) {
            String message = (String) in.readObject();
            long term = in.readLong();
            log.add(new LogEntry(message, term));
        }
    }

     */

    @Override
    public int size() {
        return log.size();
    }

    @Override
    public void add(LogEntry entry) {
        log.add(entry);
    }

    @Override
    public List<LogEntry> suffixFrom(int i) {
        if (log.isEmpty()) {
            return List.of();
        }
        var sublist = log.subList(i, log.size());
        return sublist
            .stream()
            .map(entry -> new LogEntry(entry.message(), entry.term()))
            .toList();
    }

    @Override
    public LogEntry get(int index) {
        return log.get(index);
    }

    @Override
    public void truncateToLength(int prefixLen) {
        log = log.subList(0, prefixLen);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ListLog listLog)) {
            return false;
        }
        if (size() != listLog.size()) {
            return false;
        }
        for (int i = 0; i < size(); i++) {
            if (!get(i).equals(listLog.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return log.hashCode();
    }

    @Override
    public String toString() {
        return (
            "[" +
            this.log.stream()
                .map(Record::toString)
                .collect(Collectors.joining(", ")) +
            "]"
        );
    }
}

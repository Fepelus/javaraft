package com.fepelus.raft;

import java.io.*;

interface Serialiser {
    byte[] serialise(Serializable subject);
}

interface Deserialiser {
    Event deserialise(byte[] body);
}

// use native Serialization
// (and fix the spelling)
class Serialisation implements Serialiser, Deserialiser {
    @Override
    public Event deserialise(byte[] body) {
        try {
            var ois = new ObjectInputStream(new ByteArrayInputStream(body));
            return (Event) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RaftException("Could not deserialize message", e);
        }
    }

    @Override
    public byte[] serialise(Serializable subject) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            var oos = new ObjectOutputStream(bos);
            oos.writeObject(subject);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RaftException(
                    String.format("Could not serialize event%n%s", e.getMessage()),
                    e
            );
        }
    }
}

package com.fepelus.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.*;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

/* How do you use Serialization? */
class SerializationTest {

    @Property
    boolean testLeftSerialization(@ForAll int anInteger)
        throws IOException, ClassNotFoundException {
        var thing = new LeftChild(anInteger);
        var bos = new ByteArrayOutputStream();
        var oos = new ObjectOutputStream(bos);
        oos.writeObject(thing);
        var ba = bos.toByteArray();
        var ois = new ObjectInputStream(new ByteArrayInputStream(ba));
        var newthing = (Parent) ois.readObject();
        return switch (newthing) {
            case LeftChild lc -> anInteger == lc.number();
            case RightChild _rc -> false;
        };
    }

    @Property
    boolean testRightSerialization(@ForAll List<String> strings)
        throws IOException, ClassNotFoundException {
        var thing = new RightChild(strings);
        var bos = new ByteArrayOutputStream();
        new ObjectOutputStream(bos).writeObject(thing);
        var ba = bos.toByteArray();
        var ois = new ObjectInputStream(new ByteArrayInputStream(ba));
        var newthing = (Parent) ois.readObject();
        return switch (newthing) {
            case LeftChild _lc -> false;
            case RightChild rc -> strings.equals(rc.strings());
        };
        /*
        if (newthing instanceof RightChild rc) {
            return strings.equals(rc.strings());
        } else if (newthing instanceof LeftChild) {
            return false;
        } else {
            return false;
        }
        */
    }
}

sealed interface Parent permits LeftChild, RightChild {}

record LeftChild(int number) implements Parent, Serializable {}

record RightChild(List<String> strings) implements Parent, Serializable {}

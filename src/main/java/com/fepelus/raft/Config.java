package com.fepelus.raft;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import kdl.objects.KDLDocument;
import kdl.objects.KDLNode;
import kdl.parse.KDLParser;
import kdl.search.GeneralSearch;
import kdl.search.predicates.NodePredicate;

interface Config {
    Node thisNode();

    List<? extends Node> otherNodes();

    List<? extends Node> allNodes();

    /** in milliseconds */
    int timeout();

    String dbFilename();
}

class KdlConfig implements Config {

    KDLDocument document;

    KdlConfig(InputStream stream) throws IOException {
        this.document = new KDLParser().parse(stream);
    }

    @Override
    public int timeout() {
        KDLNode timeout = getRequired("timeout");
        return timeout
            .getProps()
            .get("milliseconds")
            .getAsNumber()
            .get()
            .getValue()
            .intValue();
    }

    @Override
    public List<? extends Node> otherNodes() {
        KDLNode othernodes = getRequired("othernodes");
        if (othernodes.getChild().isEmpty()) {
            throw new RaftException(
                "Missing required children of config 'othernodes'"
            );
        }
        return othernodes
            .getChild()
            .get()
            .getNodes()
            .stream()
            .map(this::fromKdl)
            .toList();
    }

    @Override
    public List<? extends Node> allNodes() {
        return Stream.of(List.of(this.thisNode()), this.otherNodes())
            .flatMap(x -> x.stream())
            .toList();
    }

    @Override
    public String dbFilename() {
        KDLNode dbFilename = getRequired("dbFilename");
        return getStringArg(dbFilename);
    }

    KDLNode getRequired(String key) {
        List<KDLNode> nodelist = GeneralSearch.builder()
            .setMaxDepth(1)
            .setPredicate(NodePredicate.hasName(key))
            .build()
            .list(document, false)
            .getNodes();
        if (nodelist.size() == 0) {
            throw new RaftException("Missing required config " + key);
        }
        return nodelist.get(0);
    }

    @Override
    public Node thisNode() {
        KDLNode thisnode = getRequired("thisnode");
        return fromKdl(thisnode);
    }

    ConfigNode fromKdl(KDLNode node) {
        return new ConfigNode(
            getStringArg(node),
            getChildByName(node, "ip"),
            getChildByName(node, "raftport"),
            getChildByName(node, "clientport")
        );
    }

    String getChildByName(KDLNode node, String name) {
        List<KDLNode> nodes = GeneralSearch.builder()
            .setMaxDepth(2)
            .setPredicate(NodePredicate.hasName(name))
            .build()
            .list(new KDLDocument(List.of(node)), false)
            .getNodes();
        if (nodes.size() == 0) {
            throw new RaftException(node + " has no child by name " + name);
        }
        return getStringArg(nodes.get(0));
    }

    String getStringArg(KDLNode node) {
        return node.getArgs().get(0).getAsString().getValue();
    }

    @Override
    public String toString() {
        return (
            "KDLConfig[timeout:" +
            timeout() +
            ", dbFilename:" +
            dbFilename() +
            ", all:" +
            allNodes() +
            "]"
        );
    }
}

class ConfigNode implements Node, Serializable {

    String name; // ignored in equality tests
    String ip;
    String raftPort;
    String clientPort;

    ConfigNode(String name, String ip, String raftPort, String clientPort) {
        this.name = name;
        this.ip = ip;
        this.raftPort = raftPort;
        this.clientPort = clientPort;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String ip() {
        return ip;
    }

    @Override
    public String raftPort() {
        return raftPort;
    }

    @Override
    public String clientPort() {
        return clientPort;
    }

    @Override
    public String toString() {
        return (
            "Config:" +
            name +
            "[ip:" +
            ip +
            ", raftPort:" +
            raftPort +
            ", clientPort:" +
            clientPort +
            "]"
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConfigNode cn) {
            return (
                cn.ip.equals(ip) &&
                cn.raftPort.equals(raftPort) &&
                cn.clientPort.equals(clientPort)
            );
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, raftPort, clientPort);
    }
}

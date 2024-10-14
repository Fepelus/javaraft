package com.fepelus.raft;

import kdl.objects.KDLDocument;
import kdl.objects.KDLNode;
import kdl.parse.KDLParser;
import kdl.search.GeneralSearch;
import kdl.search.predicates.NodePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class Client {

    public static void main(String[] args) {
        try {
            ClientConfig config = loadConfig(args[0]);
            ClientApp client = compositionRoot(config);
            client.run(Arrays.copyOfRange(args, 1, args.length));
        } catch (RaftException e) {
            System.err.println(e);
            System.exit(1);
        }
    }

    static ClientConfig loadConfig(String filename) {
        try {
            InputStream is = new FileInputStream(filename);
            return new ClientConfig(is);
        } catch (FileNotFoundException fnfe) {
            throw new RaftException("Could not open " + filename, fnfe);
        } catch (IOException ioe) {
            throw new RaftException("Could not parse " + filename, ioe);
        }
    }

    static ClientApp compositionRoot(ClientConfig config) throws RaftException {
        Map<String, String> servers = config.servers().stream().collect(Collectors.toMap(ConfigServer::name, ConfigServer::url));
        return new ClientApp(servers);
    }
}

class ClientApp {
    private final ZContext ctx;
    private final Map<String, String> servers;
    private final Logger logger;
    private String currentLeader;

    public ClientApp(Map<String, String> servers) {
        this.servers = servers;
        this.currentLeader = selectAtRandom(servers.keySet());
        this.ctx = new ZContext();
        logger = LoggerFactory.getLogger(ClientApp.class);
    }

    private static String selectAtRandom(Set<String> names) {
        var index = new Random().ints(0, names.size()).findFirst().getAsInt();
        return names.stream().toList().get(index);
    }

    public void run(String[] command) {
        String cmd = String.join(" ", command);
        ZMQ.Socket request = ctx.createSocket(SocketType.REQ);
        request.connect(String.format("tcp://%s", servers.get(currentLeader)));

        request.send(cmd);
        String message = request.recvStr();
        request.close();
        logger.info("Received: {}", message);
        if (message.equals("retry: no leader")) {
            this.currentLeader = selectAtRandom(servers.keySet());
            run(command);
            return;
        }
        if (message.startsWith("retry: ")) {
            String[] split = message.split(": ", 2);
            if (servers.containsKey(split[1])) {
                currentLeader = split[1];
                run(command);
                return;
            }
        }
    }
}

class ClientConfig {
    private final KDLDocument document;

    public ClientConfig(InputStream stream) throws IOException {
        this.document = new KDLParser().parse(stream);
    }

    public List<ConfigServer> servers() {
        KDLNode servers = getRequired("servers");
        if (servers.getChild().isEmpty()) {
            throw new RaftException(
                    "Missing required children of config 'servers'"
            );
        }
        return servers
                .getChild()
                .get()
                .getNodes()
                .stream()
                .map(this::getConfigServer)
                .toList();
    }

    private ConfigServer getConfigServer(KDLNode node) {
        return new ConfigServer(
            node.getArgs().getFirst().getAsString().getValue(),
            node.getArgs().get(1).getAsString().getValue()
        );
    }


    private KDLNode getRequired(String key) {
        List<KDLNode> nodelist = GeneralSearch.builder()
                .setMaxDepth(1)
                .setPredicate(NodePredicate.hasName(key))
                .build()
                .list(document, false)
                .getNodes();
        if (nodelist.isEmpty()) {
            throw new RaftException("Missing required config " + key);
        }
        return nodelist.getFirst();
    }
}

record ConfigServer(String name, String url) {}
package com.fepelus.raft;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.util.*;
import kdl.objects.*;
import kdl.parse.KDLParser;
import kdl.search.*;
import kdl.search.predicates.*;
import org.junit.jupiter.api.Test;

/* How do you use KDL? */
class KdlTest {

    @Test
    void testParsingString() {
        var input = "node port=\"2\"\nnode port=\"3\"";
        KDLDocument output = new KDLParser().parse(input);
        List<KDLNode> nodes = output.getNodes();
        assertEquals(2, nodes.size());
        assertEquals(1, nodes.get(0).getProps().size());
        assertEquals("2", nodes.get(0).getProps().get("port").getValue());
    }

    @Test
    void parseTheConfigFile() throws IOException {
        InputStream contents =
            KdlTest.class.getResourceAsStream("/alice.config.kdl");
        KDLDocument document = new KDLParser().parse(contents);
        List<KDLNode> thisnodeList = GeneralSearch.builder()
            .setMaxDepth(1)
            .setPredicate(NodePredicate.hasName("thisnode"))
            .build()
            .list(document, false)
            .getNodes();
        assertEquals(4, document.getNodes().size());
        assertEquals(1, thisnodeList.size());
    }
}

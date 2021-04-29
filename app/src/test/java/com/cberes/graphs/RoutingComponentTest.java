package com.cberes.graphs;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ImpermanentDbmsExtension
class RoutingComponentTest {
    @Inject
    private GraphDatabaseService db;

    private void createLocations() {
        execute("""
                CREATE
                (a:Location {id:'a'}),
                (b:Location {id:'b'}),
                (c:Location {id:'c'}),
                (d:Location {id:'d'}),
                (e:Location {id:'e'}),
                (f:Location {id:'f'}),
                (g:Location {id:'g'}),
                (h:Location {id:'h'}),
                (i:Location {id:'i'}),
                (a)-[:CONNECTED_TO {cost:5,line:'red'}]->(b),
                (a)-[:CONNECTED_TO {cost:2,line:'green'}]->(c),
                (c)-[:CONNECTED_TO {cost:3,line:'green'}]->(d),
                (b)-[:CONNECTED_TO {cost:1,line:'red'}]->(d),
                (a)-[:CONNECTED_TO {cost:3,line:'blue'}]->(d)
                """);
    }

    private void createIndex() {
        execute("CREATE INDEX ON :Location(id)");
    }

    private void execute(final String query) {
        try (Transaction tx = db.beginTx()) {
            tx.execute(query);
            tx.commit();
        }
    }

    @Test void shortestWeightedPath() {
        createLocations();
        createIndex();
        RoutingComponent routing = new RoutingComponent(db);

        assertEquals("a->c->d",
                routing.shortestWeightedPath("a", "d", Set.of("red", "green")).get());
        assertEquals("a->d",
                routing.shortestWeightedPath("a", "d", Set.of("red", "green", "blue")).get());
        assertFalse(routing.shortestWeightedPath("a", "c", Set.of("red")).isPresent());
    }
}

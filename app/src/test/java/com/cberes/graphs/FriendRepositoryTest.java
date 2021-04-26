package com.cberes.graphs;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.*;

@ImpermanentDbmsExtension
class FriendRepositoryTest {
    /*
     * Some useful links
     * https://neo4j.com/docs/java-reference/current/java-embedded/hello-world/
     * https://neo4j.com/docs/java-reference/current/java-embedded/cypher-java/#cypher-java
     * https://github.com/neo4j/neo4j/search?q=impermanent
     * https://github.com/neo4j/neo4j/blob/e89ade8beeb5f491c218c67a0d2cfcb1c5a5c2b6/community/community-it/kernel-it/src/test/java/org/neo4j/kernel/impl/transaction/TransactionMonitorTest.java
     * https://github.com/neo4j/neo4j/blob/e89ade8beeb5f491c218c67a0d2cfcb1c5a5c2b6/community/community-it/kernel-it/src/test/java/org/neo4j/graphdb/CreateAndDeleteNodesIT.java
     */

// this is another way to the create the test database
//    GraphDatabaseService createDatabase() {
//        DatabaseManagementService managementService
//                = new TestDatabaseManagementServiceBuilder().impermanent().build();
//        GraphDatabaseService db = managementService.database();
//        // do some setup here
//        return db;
//    }

    @Inject
    private GraphDatabaseService db;

    private void createFriends() {
        execute("""
                CREATE
                (alice:Person {name:'Alice'}),
                (bob:Person {name:'Bob'}),
                (cathy:Person {name:'Cathy'}),
                (darell:Person {name:'Darell'}),
                (liz:Person {name:'Elizabeth'}),
                (ferd:Person {name:'Ferd'}),
                (gerry:Person {name:'Gerlando'}),
                (alice)-[:FRIEND]->(bob),
                (alice)-[:FRIEND]->(darell),
                (alice)-[:FRIEND]->(liz),
                (alice)-[:FRIEND]->(gerry),
                (bob)-[:FRIEND]->(alice),
                (cathy)-[:FRIEND]->(darell),
                (darell)-[:FRIEND]->(cathy),
                (darell)-[:FRIEND]->(gerry),
                (gerry)-[:FRIEND]->(liz),
                (liz)-[:FRIEND]->(gerry)
                """);
    }

    private void createIndex() {
        execute("CREATE INDEX ON :Person(name)");
    }

    private void execute(final String query) {
        try (Transaction tx = db.beginTx()) {
            tx.execute(query);
            tx.commit();
        }
    }

    @Test void friendDistance() {
        createFriends();
        createIndex();
        FriendRepository repo = new FriendRepository(db);

        assertEquals(1, repo.distance("Alice", "Bob").get());
        assertEquals(1, repo.distance("Alice", "Gerlando").get());
        assertEquals(2, repo.distance("Cathy", "Gerlando").get());
        assertEquals(3, repo.distance("Cathy", "Elizabeth").get());
        assertFalse(repo.distance("Alice", "Ferd").isPresent());
    }
}

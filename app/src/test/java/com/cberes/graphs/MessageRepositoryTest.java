package com.cberes.graphs;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.*;

@ImpermanentDbmsExtension
class MessageRepositoryTest {
    @Inject
    private GraphDatabaseService db;

    private void createPeople() {
        execute("""
                CREATE
                (alice:Person {name:'Alice'}),
                (bob:Person {name:'Bob'}),
                (cathy:Person {name:'Cathy'}),
                (darell:Person {name:'Darell'}),
                (liz:Person {name:'Elizabeth'}),
                (alice)-[:ALIAS_OF]->(bob)
                """);
    }

    private void createMessages() {
        execute("""
                MATCH (alice:Person {name:'Alice'}),
                      (bob:Person {name:'Bob'}),
                      (cathy:Person {name:'Cathy'}),
                      (darell:Person {name:'Darell'}),
                      (liz:Person {name:'Elizabeth'})
                CREATE (msg_1:Message {id:'1', content:'email contents'}),
                     (bob)-[:SENT]->(msg_1),
                     (msg_1)-[:TO]->(cathy),
                     (msg_1)-[:TO]->(darell),
                     (msg_1)-[:CC]->(alice),
                     (msg_1)-[:BCC]->(liz),
                     (msg_6:Message {id:'6', content:'email'}),
                     (bob)-[:SENT]->(msg_6),
                     (msg_6)-[:TO]->(cathy),
                     (msg_6)-[:TO]->(darell),
                     (reply_1:Message:Reply {id:'7', content:'response'}),
                     (reply_1)-[:REPLY_TO]->(msg_6),
                     (darell)-[:SENT]->(reply_1),
                     (reply_1)-[:TO]->(bob),
                     (reply_1)-[:TO]->(cathy),
                     (reply_2:Message:Reply {id:'8', content:'response'}),
                     (reply_2)-[:REPLY_TO]->(msg_6),
                     (bob)-[:SENT]->(reply_2),
                     (reply_2)-[:TO]->(darell),
                     (reply_2)-[:TO]->(cathy),
                     (reply_2)-[:CC]->(alice),
                     (reply_3:Message:Reply {id:'9', content:'response'}),
                     (reply_3)-[:REPLY_TO]->(reply_1),
                     (cathy)-[:SENT]->(reply_3),
                     (reply_3)-[:TO]->(bob),
                     (reply_3)-[:TO]->(darell),
                     (reply_4:Message:Reply {id:'10', content:'response'}),
                     (reply_4)-[:REPLY_TO]->(reply_3),
                     (bob)-[:SENT]->(reply_4),
                     (reply_4)-[:TO]->(cathy),
                     (reply_4)-[:TO]->(darell),
                     (msg_11:Message {id:'11', content:'email'}),
                     (alice)-[:SENT]->(msg_11)-[:TO]->(bob),
                     (msg_12:Message:Forward {id:'12', content:'email'}),
                     (msg_12)-[:FORWARD_OF]->(msg_11),
                     (bob)-[:SENT]->(msg_12)-[:TO]->(cathy),
                     (msg_13:Message:Forward {id:'13', content:'email'}),
                     (msg_13)-[:FORWARD_OF]->(msg_12),
                     (cathy)-[:SENT]->(msg_13)-[:TO]->(darell)
                """);
    }

    private void createIndex() {
        execute("CREATE INDEX ON :Message(id)");
    }

    private void createUnique() {
        execute("CREATE CONSTRAINT ON (m:Message) ASSERT m.id IS UNIQUE");
    }

    private void execute(final String query) {
        try (Transaction tx = db.beginTx()) {
            tx.execute(query);
            tx.commit();
        }
    }

    @Test void uniqueWorks() {
        createPeople();
        createMessages();
        createUnique(); // cannot have an index and a constraint
        QueryExecutionException e = assertThrows(QueryExecutionException.class,
                () -> execute("CREATE (msg_1:Message {id:'1', content:'duplicate message'})"));
        assertTrue(e.getMessage().contains(" already exists "));
    }

    @Test void toUsingCollect() {
        createPeople();
        createMessages();
        createIndex();
        MessageRepository repo = new MessageRepository(db);

        Optional<String> to = repo.to("1");
        assertTrue(to.isPresent());
        assertEquals("[Cathy, Darell]", to.get());
    }

    @Test void forwardCount() {
        createPeople();
        createMessages();
        createIndex();
        MessageRepository repo = new MessageRepository(db);

        Optional<Long> count = repo.forwardCount("11");
        assertTrue(count.isPresent());
        assertEquals(2, count.get());
    }

    @Test void ccToAlias() {
        createPeople();
        createMessages();
        createIndex();
        MessageRepository repo = new MessageRepository(db);

        Set<String> ccs = repo.ccAlias("Bob");
        assertEquals(2, ccs.size());
        assertTrue(ccs.contains("1"));
        assertTrue(ccs.contains("8"));
    }

    @Test void replies() {
        createPeople();
        createMessages();
        createIndex();
        MessageRepository repo = new MessageRepository(db);

        List<MessageRepository.Reply> replies = repo.replies("6");
        assertEquals(4, replies.size());
        assertEquals("Darell", replies.get(0).name());
        assertEquals(1, replies.get(0).depth());
        assertEquals("Bob", replies.get(1).name());
        assertEquals(1, replies.get(1).depth());
        assertEquals("Cathy", replies.get(2).name());
        assertEquals(2, replies.get(2).depth());
        assertEquals("Bob", replies.get(3).name());
        assertEquals(3, replies.get(3).depth());
    }
}

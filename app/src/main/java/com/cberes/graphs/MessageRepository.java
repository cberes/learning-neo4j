package com.cberes.graphs;

import java.util.*;

import org.neo4j.graphdb.GraphDatabaseService;

public class MessageRepository {
    public static record Reply(String name, Long depth) {}

    private final GraphDatabaseService db;

    public MessageRepository(final GraphDatabaseService db) {
        this.db = db;
    }

    public Set<String> ccAlias(final String personName) {
        String query = """
                MATCH (person:Person {name:$name})-[:SENT]->(msg)-[:CC]->(alias),
                    (alias)-[:ALIAS_OF]->(person)
                RETURN msg.id AS id
                """;

        Map<String, Object> params = Map.of("name", personName);
        return db.executeTransactionally(query, params, result -> {
            Set<String> results = new HashSet<>();
            while (result.hasNext()) {
                results.add(result.next().get("id").toString());
            }
            return results;
        });
    }

    public Optional<String> to(final String msgId) {
        String query = """
                MATCH (msg:Message {id:$msgId})-[:TO]->(to)
                RETURN collect(to.name) AS names
                """;

        Map<String, Object> params = Map.of("msgId", msgId);
        return db.executeTransactionally(query, params, r -> {
            if (r.hasNext()) {
                return Optional.of(r.next().get("names").toString());
            } else {
                return Optional.empty();
            }
        });
    }

    public Optional<Long> forwardCount(final String msgId) {
        String query = """
                MATCH (msg:Message {id:$msgId})<-[f:FORWARD_OF*]->(:Forward)
                RETURN count(f)
                """;

        Map<String, Object> params = Map.of("msgId", msgId);
        return db.executeTransactionally(query, params, r -> {
            if (r.hasNext()) {
                return Optional.of((Long) r.next().get("count(f)"));
            } else {
                return Optional.empty();
            }
        });
    }

    public List<Reply> replies(final String msgId) {
        String query = """
                MATCH p=(msg:Message {id:$msgId})<-[:REPLY_TO*1..4]-(:Reply)<-[:SENT]-(replier)
                RETURN replier.name AS replier, length(p) - 1 AS depth
                ORDER BY depth
                """;

        Map<String, Object> params = Map.of("msgId", msgId);
        return db.executeTransactionally(query, params, r -> {
            List<Reply> results = new LinkedList<>();
            while (r.hasNext()) {
                Map<String, ?> result = r.next();
                results.add(new Reply(result.get("replier").toString(), (Long) result.get("depth")));
            }
            return results;
        });
    }
}

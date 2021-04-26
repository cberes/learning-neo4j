package com.cberes.graphs;

import java.util.Map;
import java.util.Optional;

import org.neo4j.graphdb.GraphDatabaseService;

public class FriendRepository {
    private final GraphDatabaseService db;

    public FriendRepository(final GraphDatabaseService db) {
        this.db = db;
    }

    public Optional<Long> distance(final String name1, final String name2) {
        String query = """
                MATCH (first:Person {name:$friend1}),
                (second:Person {name:$friend2})
                MATCH p=shortestPath((first)-[*..4]-(second))
                RETURN length(p) AS distance
                """;

        Map<String, Object> params = Map.of("friend1", name1, "friend2", name2);
        return db.executeTransactionally(query, params,
                result -> result.hasNext() ? Optional.of((Long) result.next().get("distance")) : Optional.empty());
    }
}

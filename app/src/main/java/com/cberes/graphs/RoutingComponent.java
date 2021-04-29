package com.cberes.graphs;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import org.neo4j.graphdb.GraphDatabaseService;

public class RoutingComponent {
    private final GraphDatabaseService db;

    public RoutingComponent(final GraphDatabaseService db) {
        this.db = db;
    }

    public Optional<String> shortestWeightedPath(final String start, final String end, final Collection<String> lines) {
        String query = """
                MATCH (s:Location {id:$start}),
                (e:Location {id:$end})
                MATCH route = (s)-[CONNECTED_TO*]-(e)
                WHERE all(r in relationships(route)
                    WHERE r.line IN $lines)
                WITH s, route, reduce(weight = 0, r in relationships(route) | weight + r.cost) AS score
                ORDER BY score ASC
                LIMIT 1
                RETURN reduce(path = head(nodes(route)).id, n in tail(nodes(route)) | path + '->' + n.id) AS z
                """; // we can concatenate routes like this: nodes(a) + nodes(b)

        Map<String, Object> params = Map.of("start", start, "end", end, "lines", lines);
        return db.executeTransactionally(query, params,
                result -> result.hasNext() ? Optional.of(result.next().get("z").toString()) : Optional.empty());
    }
}

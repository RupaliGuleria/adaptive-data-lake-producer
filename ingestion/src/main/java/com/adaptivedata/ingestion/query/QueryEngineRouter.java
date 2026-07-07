package com.adaptivedata.ingestion.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Routes a {@link QueryFilter} to the appropriate engine.
 *
 * <p>Routing logic:
 * <ul>
 *   <li>If {@code filter.preferSpark} is true AND Spark is configured → Spark
 *   <li>Everything else → DuckDB
 * </ul>
 *
 * <p>This is the single entry point for Phase 8 REST controllers — they call
 * {@code router.execute(filter)} without knowing which engine runs the query.
 */
@Component
public class QueryEngineRouter {

    private static final Logger logger = LoggerFactory.getLogger(QueryEngineRouter.class);

    private final DuckDbQueryEngine duckDb;
    private final Optional<SparkQueryEngine> spark;

    public QueryEngineRouter(DuckDbQueryEngine duckDb, Optional<SparkQueryEngine> spark) {
        this.duckDb = duckDb;
        this.spark = spark;
        logger.info("QueryEngineRouter ready | duckdb=active spark={}",
                spark.isPresent() ? "active" : "disabled");
    }

    public QueryResult execute(QueryFilter filter) {
        QueryEngine engine = selectEngine(filter);
        logger.debug("Routing query | engine={} prefix={} preferSpark={}",
                engine.engineName(), filter.getPrefix(), filter.isPreferSpark());
        return engine.execute(filter);
    }

    private QueryEngine selectEngine(QueryFilter filter) {
        if (filter.isPreferSpark() && spark.isPresent()) {
            return spark.get();
        }
        return duckDb;
    }
}

package com.adaptivedata.ingestion.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Spark query engine — scaffold for future distributed query support.
 *
 * <p><b>Activation:</b> set {@code ingestion.query.spark.enabled=true} in application.yml.
 * By default this bean is not created, so no Spark context is started.
 *
 * <p><b>To fully implement:</b>
 * <ol>
 *   <li>Add to pom.xml: {@code org.apache.spark:spark-sql_2.12:3.5.x} and
 *       {@code org.apache.hadoop:hadoop-aws:3.3.6}
 *   <li>Create a {@code SparkSession} @Bean in a {@code SparkConfig} class, configured
 *       with MinIO's S3A endpoint ({@code fs.s3a.endpoint}, {@code fs.s3a.path.style.access=true})
 *   <li>Replace the {@code execute()} stub with a real
 *       {@code sparkSession.read().parquet(s3aPath).where(filter)} call
 * </ol>
 *
 * <p>Partition path format for Spark: {@code s3a://bucket/data/year=.../.../*.parquet}
 * (same Hive layout, different scheme — Spark uses S3A, DuckDB uses S3).
 */
@Component
@ConditionalOnProperty(name = "ingestion.query.spark.enabled", havingValue = "true", matchIfMissing = false)
public class SparkQueryEngine implements QueryEngine {

    private static final Logger logger = LoggerFactory.getLogger(SparkQueryEngine.class);
    private static final String ENGINE = "spark";

    public SparkQueryEngine() {
        logger.warn("SparkQueryEngine is enabled but not yet fully implemented. " +
                "Add spark-sql_2.12 and hadoop-aws to pom.xml before use.");
    }

    @Override
    public String engineName() {
        return ENGINE;
    }

    @Override
    public QueryResult execute(QueryFilter filter) {
        throw new UnsupportedOperationException(
                "SparkQueryEngine is not yet implemented. " +
                "Add spark-sql_2.12:3.5.x and hadoop-aws:3.3.6 to pom.xml, " +
                "then implement this method using SparkSession.read().parquet(s3aPath).");
    }
}

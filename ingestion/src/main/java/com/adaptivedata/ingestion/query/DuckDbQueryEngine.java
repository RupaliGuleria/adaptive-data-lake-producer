package com.adaptivedata.ingestion.query;

import com.adaptivedata.ingestion.config.MinioProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Query engine backed by DuckDB's in-process SQL runtime.
 *
 * <p>DuckDB reads Parquet files directly from MinIO via its {@code httpfs} extension,
 * which supports S3-compatible endpoints. {@code hive_partitioning=true} turns the
 * directory segments ({@code year=, month=, ...}) into queryable columns and enables
 * DuckDB to skip entire directories that don't match the WHERE clause.
 *
 * <p>A single shared {@link Connection} is configured once at startup and reused.
 * Access is synchronised because the JDBC connection is not thread-safe; the Phase 8
 * REST layer should introduce a connection-per-request or HikariCP pool if concurrency
 * becomes a bottleneck.
 */
@Component
public class DuckDbQueryEngine implements QueryEngine {

    private static final Logger logger = LoggerFactory.getLogger(DuckDbQueryEngine.class);
    private static final String ENGINE = "duckdb";

    private final MinioProperties minioProps;
    private final PartitionPathBuilder pathBuilder;

    @Value("${ingestion.query.max-rows:10000}")
    private int globalMaxRows;

    private Connection connection;

    public DuckDbQueryEngine(MinioProperties minioProps) {
        this.minioProps = minioProps;
        this.pathBuilder = new PartitionPathBuilder(minioProps.getBucket());
    }

    @PostConstruct
    public void init() throws Exception {
        Class.forName("org.duckdb.DuckDBDriver");
        connection = DriverManager.getConnection("jdbc:duckdb:");
        configureS3(connection);
        logger.info("DuckDB engine ready | endpoint={} bucket={}",
                minioProps.getEndpoint(), minioProps.getBucket());
    }

    @PreDestroy
    public void shutdown() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public String engineName() {
        return ENGINE;
    }

    @Override
    public synchronized QueryResult execute(QueryFilter filter) {
        String s3Path = pathBuilder.build(filter);
        String whereClause = pathBuilder.buildWhere(filter);
        int pageSize = Math.min(filter.getPageSize(), Math.min(filter.getMaxRows(), globalMaxRows));
        int offset = (filter.getPage() - 1) * pageSize;

        String scanSource = "read_parquet('" + s3Path + "', hive_partitioning=true)";
        String countSql = buildCountSql(scanSource, whereClause);
        String sql = buildSql(scanSource, whereClause, pageSize, offset);

        logger.info("DuckDB query | path={} where=[{}] page={} pageSize={} offset={}",
                s3Path, whereClause, filter.getPage(), pageSize, offset);

        long start = System.currentTimeMillis();
        try (Statement stmt = connection.createStatement()) {
            int totalRows = queryCount(stmt, countSql);

            List<String> columns;
            List<Map<String, Object>> rows = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                columns = new ArrayList<>(colCount);
                for (int i = 1; i <= colCount; i++) {
                    columns.add(meta.getColumnName(i));
                }

                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>(colCount);
                    for (String col : columns) {
                        row.put(col, rs.getObject(col));
                    }
                    rows.add(row);
                }
            }

            long ms = System.currentTimeMillis() - start;
            int totalPages = Math.max(1, (int) Math.ceil(totalRows / (double) pageSize));
            logger.info("DuckDB result | returned={} totalRows={} totalPages={} time_ms={}",
                    rows.size(), totalRows, totalPages, ms);

            return QueryResult.builder()
                    .columns(columns)
                    .rows(rows)
                    .totalRows(totalRows)
                    .returnedRows(rows.size())
                    .page(filter.getPage())
                    .pageSize(pageSize)
                    .totalPages(totalPages)
                    .executionTimeMs(ms)
                    .engine(ENGINE)
                    .scannedPath(s3Path)
                    .build();

        } catch (Exception e) {
            long ms = System.currentTimeMillis() - start;
            logger.error("DuckDB query failed | path={} time_ms={}", s3Path, ms, e);
            throw new QueryExecutionException("DuckDB query failed: " + e.getMessage(), e);
        }
    }

    private int queryCount(Statement stmt, String countSql) throws Exception {
        try (ResultSet rs = stmt.executeQuery(countSql)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private String buildSql(String scanSource, String whereClause, int limit, int offset) {
        StringBuilder sql = new StringBuilder()
                .append("SELECT * FROM ").append(scanSource);

        if (!whereClause.isBlank()) {
            sql.append(" WHERE ").append(whereClause);
        }

        sql.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
        return sql.toString();
    }

    private String buildCountSql(String scanSource, String whereClause) {
        StringBuilder sql = new StringBuilder()
                .append("SELECT COUNT(*) FROM ").append(scanSource);

        if (!whereClause.isBlank()) {
            sql.append(" WHERE ").append(whereClause);
        }

        return sql.toString();
    }

    private void configureS3(Connection conn) throws Exception {
        URI endpoint = URI.create(minioProps.getEndpoint());
        String host = endpoint.getHost();
        int port = endpoint.getPort();
        boolean useSsl = "https".equalsIgnoreCase(endpoint.getScheme());

        try (Statement stmt = conn.createStatement()) {
            // INSTALL is best-effort — httpfs is bundled in the JDBC JAR and may not need downloading
            try {
                stmt.execute("INSTALL httpfs");
            } catch (Exception e) {
                logger.debug("httpfs install skipped (likely already bundled): {}", e.getMessage());
            }
            stmt.execute("LOAD httpfs");
            stmt.execute(String.format("SET s3_endpoint='%s:%d'", host, port));
            stmt.execute(String.format("SET s3_access_key_id='%s'", minioProps.getAccessKey()));
            stmt.execute(String.format("SET s3_secret_access_key='%s'", minioProps.getSecretKey()));
            stmt.execute("SET s3_url_style='path'");
            stmt.execute("SET s3_use_ssl=" + useSsl);
        }
    }
}

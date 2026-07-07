package com.adaptivedata.ingestion.query;

/**
 * Builds the S3 glob path used in DuckDB's {@code read_parquet()} call.
 *
 * <p>Partition layout on MinIO mirrors the Hive convention written by {@code ParquetWriteBuffer}:
 * <pre>
 *   {prefix}/year=YYYY/month=MM/day=DD/hour=HH/source={src}/part-N-T.parquet
 * </pre>
 *
 * <p>The more partition levels specified in the filter the more directories DuckDB
 * can skip before reading a single byte of column data. Only contiguous prefixes
 * can be pushed into the path (no gaps), so the builder stops adding segments at
 * the first missing filter field.
 */
public class PartitionPathBuilder {

    private final String bucket;

    public PartitionPathBuilder(String bucket) {
        this.bucket = bucket;
    }

    /**
     * Returns the deepest S3 glob that the supplied filter allows.
     * Example outputs:
     * <ul>
     *   <li>No partitions set → {@code s3://bucket/data/**}{@code /*.parquet}</li>
     *   <li>year=2026 only   → {@code s3://bucket/data/year=2026/**}{@code /*.parquet}</li>
     *   <li>year+month+day   → {@code s3://bucket/data/year=2026/month=07/day=01/**}{@code /*.parquet}</li>
     * </ul>
     */
    public String build(QueryFilter filter) {
        StringBuilder path = new StringBuilder("s3://")
                .append(bucket)
                .append("/")
                .append(filter.getPrefix());

        if (filter.getYear() != null) {
            path.append("/year=").append(filter.getYear());

            if (filter.getMonth() != null) {
                path.append(String.format("/month=%02d", filter.getMonth()));

                if (filter.getDay() != null) {
                    path.append(String.format("/day=%02d", filter.getDay()));

                    if (filter.getHour() != null) {
                        path.append(String.format("/hour=%02d", filter.getHour()));

                        if (filter.getSource() != null && !filter.getSource().isBlank()) {
                            path.append("/source=").append(filter.getSource());
                        }
                    }
                }
            }
        }

        path.append("/**/*.parquet");
        return path.toString();
    }

    /**
     * Builds a SQL WHERE clause fragment for any filter fields that could not be
     * embedded in the path (e.g. source when hour is not specified), plus any
     * caller-supplied {@code additionalWhere} predicate.
     *
     * <p>With DuckDB's {@code hive_partitioning=true} all partition directory names
     * become columns, so these predicates still benefit from column pruning at the
     * row-group level inside each Parquet file.
     */
    public String buildWhere(QueryFilter filter) {
        StringBuilder where = new StringBuilder();

        // source pushed only when it could not be embedded in the path
        if (filter.getSource() != null && !filter.getSource().isBlank() && filter.getHour() == null) {
            appendAnd(where, "source = '" + filter.getSource() + "'");
        }

        if (filter.getAdditionalWhere() != null && !filter.getAdditionalWhere().isBlank()) {
            appendAnd(where, "(" + filter.getAdditionalWhere() + ")");
        }

        return where.toString();
    }

    private void appendAnd(StringBuilder sb, String clause) {
        if (!sb.isEmpty()) {
            sb.append(" AND ");
        }
        sb.append(clause);
    }
}

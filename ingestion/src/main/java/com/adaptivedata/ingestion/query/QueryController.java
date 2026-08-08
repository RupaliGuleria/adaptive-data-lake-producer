package com.adaptivedata.ingestion.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Thin HTTP façade over {@link QueryEngineRouter}.
 *
 * <p>Two access styles:
 * <ul>
 *   <li>GET  /api/v1/query  — quick ad-hoc queries via query params (browser / curl friendly)
 *   <li>POST /api/v1/query  — structured QueryFilter as JSON body (programmatic use)
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/query")
public class QueryController {

    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    private final QueryEngineRouter router;

    public QueryController(QueryEngineRouter router) {
        this.router = router;
    }

    /**
     * Ad-hoc query via URL params.
     * Example:
     * <pre>
     *   GET /api/v1/query?prefix=data&year=2026&month=7&maxRows=50
     *   GET /api/v1/query?prefix=quarantine&year=2026&month=7&where=quality_score < 0.6
     * </pre>
     */
    @GetMapping
    public ResponseEntity<QueryResult> query(
            @RequestParam(defaultValue = "data")    String  prefix,
            @RequestParam(required = false)         Integer year,
            @RequestParam(required = false)         Integer month,
            @RequestParam(required = false)         Integer day,
            @RequestParam(required = false)         Integer hour,
            @RequestParam(required = false)         String  source,
            @RequestParam(required = false)         String  where,
            @RequestParam(defaultValue = "100")     int     maxRows,
            @RequestParam(defaultValue = "false")   boolean preferSpark) {

        QueryFilter filter = QueryFilter.builder()
                .prefix(prefix)
                .year(year).month(month).day(day).hour(hour)
                .source(source)
                .additionalWhere(where)
                .maxRows(maxRows)
                .preferSpark(preferSpark)
                .build();

        return execute(filter);
    }

    /**
     * Structured query via JSON body.
     * Example body:
     * <pre>
     * {
     *   "prefix": "data",
     *   "year": 2026, "month": 7,
     *   "additionalWhere": "quality_score >= 0.9",
     *   "maxRows": 200
     * }
     * </pre>
     */
    @PostMapping
    public ResponseEntity<QueryResult> queryPost(@RequestBody QueryFilter filter) {
        return execute(filter);
    }

    private ResponseEntity<QueryResult> execute(QueryFilter filter) {
        try {
            QueryResult result = router.execute(filter);
            return ResponseEntity.ok(result);
        } catch (QueryExecutionException e) {
            logger.error("Query failed | prefix={}", filter.getPrefix(), e);
            return ResponseEntity.internalServerError()
                    .body(QueryResult.builder()
                            .columns(java.util.List.of("error"))
                            .rows(java.util.List.of(Map.of("error", e.getMessage())))
                            .totalRows(0)
                            .executionTimeMs(0)
                            .engine("none")
                            .scannedPath("n/a")
                            .build());
        }
    }
}

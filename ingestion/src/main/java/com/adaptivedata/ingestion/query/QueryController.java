package com.adaptivedata.ingestion.query;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Thin HTTP façade over {@link QueryEngineRouter}.
 *
 * <p>Two access styles:
 * <ul>
 *   <li>GET  /api/v1/query  — quick ad-hoc queries via query params (browser / curl friendly)
 *   <li>POST /api/v1/query  — structured QueryFilter as JSON body (programmatic use)
 * </ul>
 *
 * <p>Parameter and body validation is enforced here ({@code @Validated} / {@code @Valid});
 * failures and any downstream query errors are translated into a standardised
 * {@link com.adaptivedata.ingestion.web.ApiError} body by
 * {@link com.adaptivedata.ingestion.web.GlobalExceptionHandler}.
 */
@RestController
@RequestMapping("/api/v1/query")
@Validated
public class QueryController {

    private final QueryEngineRouter router;

    public QueryController(QueryEngineRouter router) {
        this.router = router;
    }

    /**
     * Ad-hoc query via URL params.
     * Example:
     * <pre>
     *   GET /api/v1/query?prefix=data&year=2026&month=7&pageSize=50
     *   GET /api/v1/query?prefix=quarantine&year=2026&month=7&where=quality_score < 0.6
     * </pre>
     */
    @GetMapping
    public ResponseEntity<QueryResult> query(
            @RequestParam(defaultValue = "data")    String  prefix,
            @RequestParam(required = false) @Min(2000) @Max(2100) Integer year,
            @RequestParam(required = false) @Min(1) @Max(12)      Integer month,
            @RequestParam(required = false) @Min(1) @Max(31)      Integer day,
            @RequestParam(required = false) @Min(0) @Max(23)      Integer hour,
            @RequestParam(required = false)         String  source,
            @RequestParam(required = false)         String  where,
            @RequestParam(defaultValue = "10000")   @Min(1) @Max(100_000) int maxRows,
            @RequestParam(defaultValue = "1")       @Min(1) int page,
            @RequestParam(defaultValue = "100")     @Min(1) @Max(10_000) int pageSize,
            @RequestParam(defaultValue = "false")   boolean preferSpark) {

        QueryFilter filter = QueryFilter.builder()
                .prefix(prefix)
                .year(year).month(month).day(day).hour(hour)
                .source(source)
                .additionalWhere(where)
                .maxRows(maxRows)
                .page(page)
                .pageSize(pageSize)
                .preferSpark(preferSpark)
                .build();

        return ResponseEntity.ok(router.execute(filter));
    }

    /**
     * Structured query via JSON body.
     * Example body:
     * <pre>
     * {
     *   "prefix": "data",
     *   "year": 2026, "month": 7,
     *   "additionalWhere": "quality_score >= 0.9",
     *   "page": 1, "pageSize": 200
     * }
     * </pre>
     */
    @PostMapping
    public ResponseEntity<QueryResult> queryPost(@Valid @RequestBody QueryFilter filter) {
        return ResponseEntity.ok(router.execute(filter));
    }
}

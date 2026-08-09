package com.adaptivedata.ingestion.query;

import com.adaptivedata.ingestion.web.GlobalExceptionHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * MockMvc slice test for {@link QueryController}. Exercises the full REST layer —
 * routing, pagination param wiring, bean validation, and {@link GlobalExceptionHandler}
 * error mapping — without a real DuckDB/MinIO backend ({@link QueryEngineRouter} is mocked).
 */
@WebMvcTest(QueryController.class)
@Import(GlobalExceptionHandler.class)
class QueryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private QueryEngineRouter router;

    // -----------------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------------

    @Test
    void get_validParams_returnsQueryResult() throws Exception {
        when(router.execute(any(QueryFilter.class))).thenReturn(sampleResult());

        mockMvc.perform(get("/api/v1/query")
                        .param("prefix", "data")
                        .param("year", "2026")
                        .param("month", "7")
                        .param("page", "2")
                        .param("pageSize", "50"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.engine").value("duckdb"))
                .andExpect(jsonPath("$.page").value(2))
                .andExpect(jsonPath("$.pageSize").value(50))
                .andExpect(jsonPath("$.totalPages").value(3))
                .andExpect(jsonPath("$.totalRows").value(125))
                .andExpect(jsonPath("$.rows[0].transaction_id").value("T1"));
    }

    @Test
    void get_paramsBuildCorrectFilter() throws Exception {
        when(router.execute(any(QueryFilter.class))).thenReturn(sampleResult());

        mockMvc.perform(get("/api/v1/query")
                        .param("prefix", "quarantine")
                        .param("year", "2026")
                        .param("month", "7")
                        .param("day", "1")
                        .param("hour", "10")
                        .param("source", "cloud")
                        .param("where", "quality_score < 0.6")
                        .param("maxRows", "500")
                        .param("page", "3")
                        .param("pageSize", "25")
                        .param("preferSpark", "true"))
                .andExpect(status().isOk());

        org.mockito.ArgumentCaptor<QueryFilter> captor = org.mockito.ArgumentCaptor.forClass(QueryFilter.class);
        verify(router).execute(captor.capture());
        QueryFilter filter = captor.getValue();

        org.assertj.core.api.Assertions.assertThat(filter.getPrefix()).isEqualTo("quarantine");
        org.assertj.core.api.Assertions.assertThat(filter.getYear()).isEqualTo(2026);
        org.assertj.core.api.Assertions.assertThat(filter.getMonth()).isEqualTo(7);
        org.assertj.core.api.Assertions.assertThat(filter.getDay()).isEqualTo(1);
        org.assertj.core.api.Assertions.assertThat(filter.getHour()).isEqualTo(10);
        org.assertj.core.api.Assertions.assertThat(filter.getSource()).isEqualTo("cloud");
        org.assertj.core.api.Assertions.assertThat(filter.getAdditionalWhere()).isEqualTo("quality_score < 0.6");
        org.assertj.core.api.Assertions.assertThat(filter.getMaxRows()).isEqualTo(500);
        org.assertj.core.api.Assertions.assertThat(filter.getPage()).isEqualTo(3);
        org.assertj.core.api.Assertions.assertThat(filter.getPageSize()).isEqualTo(25);
        org.assertj.core.api.Assertions.assertThat(filter.isPreferSpark()).isTrue();
    }

    @Test
    void get_defaults_appliedWhenParamsOmitted() throws Exception {
        when(router.execute(any(QueryFilter.class))).thenReturn(sampleResult());

        mockMvc.perform(get("/api/v1/query")).andExpect(status().isOk());

        org.mockito.ArgumentCaptor<QueryFilter> captor = org.mockito.ArgumentCaptor.forClass(QueryFilter.class);
        verify(router).execute(captor.capture());
        QueryFilter filter = captor.getValue();

        org.assertj.core.api.Assertions.assertThat(filter.getPrefix()).isEqualTo("data");
        org.assertj.core.api.Assertions.assertThat(filter.getPage()).isEqualTo(1);
        org.assertj.core.api.Assertions.assertThat(filter.getPageSize()).isEqualTo(100);
        org.assertj.core.api.Assertions.assertThat(filter.getMaxRows()).isEqualTo(10_000);
        org.assertj.core.api.Assertions.assertThat(filter.isPreferSpark()).isFalse();
    }

    @Test
    void post_validBody_returnsQueryResult() throws Exception {
        when(router.execute(any(QueryFilter.class))).thenReturn(sampleResult());

        String body = """
                {"prefix":"data","year":2026,"month":7,"page":1,"pageSize":100}
                """;

        mockMvc.perform(post("/api/v1/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.engine").value("duckdb"));
    }

    // -----------------------------------------------------------------------
    // Validation failures → 400 ApiError
    // -----------------------------------------------------------------------

    @Test
    void get_monthOutOfRange_returns400WithValidationError() throws Exception {
        mockMvc.perform(get("/api/v1/query").param("month", "13"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.path").value("/api/v1/query"));
    }

    @Test
    void get_yearOutOfRange_returns400() throws Exception {
        mockMvc.perform(get("/api/v1/query").param("year", "1999"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
    }

    @Test
    void get_pageSizeAboveLimit_returns400() throws Exception {
        mockMvc.perform(get("/api/v1/query").param("pageSize", "999999"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
    }

    @Test
    void get_pageBelowOne_returns400() throws Exception {
        mockMvc.perform(get("/api/v1/query").param("page", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));
    }

    @Test
    void get_nonNumericYear_returns400TypeMismatch() throws Exception {
        mockMvc.perform(get("/api/v1/query").param("year", "not-a-year"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("TYPE_MISMATCH"));
    }

    @Test
    void post_bodyWithInvalidMonth_returns400WithFieldDetail() throws Exception {
        String body = """
                {"prefix":"data","month":15}
                """;

        mockMvc.perform(post("/api/v1/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                .andExpect(jsonPath("$.details[0]").value(org.hamcrest.Matchers.containsString("month")));
    }

    @Test
    void post_malformedJson_returns400() throws Exception {
        mockMvc.perform(post("/api/v1/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not valid json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("MALFORMED_REQUEST"));
    }

    // -----------------------------------------------------------------------
    // Downstream failure → 500 ApiError
    // -----------------------------------------------------------------------

    @Test
    void get_queryEngineThrows_returns500WithQueryExecutionError() throws Exception {
        when(router.execute(any(QueryFilter.class)))
                .thenThrow(new QueryExecutionException("DuckDB query failed: connection refused", null));

        mockMvc.perform(get("/api/v1/query"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("QUERY_EXECUTION_ERROR"))
                .andExpect(jsonPath("$.message").value("DuckDB query failed: connection refused"));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private QueryResult sampleResult() {
        return QueryResult.builder()
                .columns(List.of("transaction_id", "amount"))
                .rows(List.of(Map.of("transaction_id", "T1", "amount", 100.0)))
                .totalRows(125)
                .returnedRows(1)
                .page(2)
                .pageSize(50)
                .totalPages(3)
                .executionTimeMs(12)
                .engine("duckdb")
                .scannedPath("s3://bucket/data/year=2026/month=07/**/*.parquet")
                .build();
    }
}

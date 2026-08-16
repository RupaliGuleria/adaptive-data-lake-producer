package com.adaptivedata.ingestion.intelligence.quality;

import com.adaptivedata.ingestion.intelligence.quality.rules.NotNullRule;
import com.adaptivedata.ingestion.intelligence.quality.rules.PositiveNumberRule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers parameterized (NotNullRule, PositiveNumberRule) rule instances as Spring beans,
 * one per field per schema_id. Self-contained rules (@Component) are auto-discovered and
 * do not need entries here, but must still scope themselves via {@link QualityRule#appliesTo}.
 */
@Configuration
public class QualityRulesConfig {

    private static final String BANKING = "banking_transaction_v1";
    private static final String TPCH_ORDERS = "tpch_orders_v1";
    private static final String TPCH_LINEITEM = "tpch_lineitem_v1";

    @Bean
    public QualityRule transactionIdNotNull() {
        return new NotNullRule("transaction_id", Severity.CRITICAL, BANKING);
    }

    @Bean
    public QualityRule transactionDateNotNull() {
        return new NotNullRule("transaction_date", Severity.CRITICAL, BANKING);
    }

    @Bean
    public QualityRule amountNotNull() {
        return new NotNullRule("amount", Severity.CRITICAL, BANKING);
    }

    @Bean
    public QualityRule amountPositive() {
        return new PositiveNumberRule("amount", Severity.HIGH, BANKING);
    }

    @Bean
    public QualityRule ordersOrderKeyNotNull() {
        return new NotNullRule("o_orderkey", Severity.CRITICAL, TPCH_ORDERS);
    }

    @Bean
    public QualityRule ordersTotalPriceNotNull() {
        return new NotNullRule("o_totalprice", Severity.CRITICAL, TPCH_ORDERS);
    }

    @Bean
    public QualityRule ordersTotalPricePositive() {
        return new PositiveNumberRule("o_totalprice", Severity.HIGH, TPCH_ORDERS);
    }

    @Bean
    public QualityRule lineitemLineItemIdNotNull() {
        return new NotNullRule("line_item_id", Severity.CRITICAL, TPCH_LINEITEM);
    }

    @Bean
    public QualityRule lineitemOrderKeyNotNull() {
        return new NotNullRule("l_orderkey", Severity.CRITICAL, TPCH_LINEITEM);
    }

    @Bean
    public QualityRule lineitemQuantityPositive() {
        return new PositiveNumberRule("l_quantity", Severity.HIGH, TPCH_LINEITEM);
    }
}

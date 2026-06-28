package com.adaptivedata.ingestion.intelligence.quality;

import com.adaptivedata.ingestion.intelligence.quality.rules.NotNullRule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers parameterized NotNullRule instances as Spring beans.
 * Self-contained rules (@Component) are auto-discovered and do not need entries here.
 */
@Configuration
public class QualityRulesConfig {

    @Bean
    public QualityRule transactionIdNotNull() {
        return new NotNullRule("transaction_id", Severity.CRITICAL);
    }

    @Bean
    public QualityRule transactionDateNotNull() {
        return new NotNullRule("transaction_date", Severity.CRITICAL);
    }

    @Bean
    public QualityRule amountNotNull() {
        return new NotNullRule("amount", Severity.CRITICAL);
    }
}

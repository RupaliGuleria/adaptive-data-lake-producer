package com.adaptivedata.ingestion.intelligence.quality;

import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class QualityCheckResult {
    double qualityScore;
    boolean passed;
    double threshold;
    List<RuleResult> ruleResults;
}

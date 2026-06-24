package com.adaptivedata.ingestion.coordinator;

import com.adaptivedata.ingestion.model.BatchStatus;
import com.adaptivedata.ingestion.model.ProcessedEvent;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchState {

    private final String tradeGroupId;
    private final int expectedCount;
    private final Set<String> expectedTradeIds;
    private final Set<String> receivedTradeIds = ConcurrentHashMap.newKeySet();
    private final AtomicInteger actualCount = new AtomicInteger(0);
    private final Queue<ProcessedEvent> events = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private volatile BatchStatus status = BatchStatus.PENDING;
    private final Instant deadline;

    public BatchState(String tradeGroupId, int expectedCount, Set<String> expectedTradeIds, Instant deadline) {
        this.tradeGroupId = tradeGroupId;
        this.expectedCount = expectedCount;
        this.expectedTradeIds = expectedTradeIds;
        this.deadline = deadline;
    }

    public void addEvent(ProcessedEvent event) {
        // ConcurrentHashMap.KeySetView.add() is atomic (backed by putIfAbsent).
        // Guards against two workers racing through the DeduplicationStore check
        // within the same poll batch — only the first add for a given tradeId wins.
        if (receivedTradeIds.add(event.getTradeId())) {
            events.add(event);
            actualCount.incrementAndGet();
        }
    }

    /**
     * Attempts to transition this batch to SUCCESS. Returns true exactly once
     * when both count and trade_id set match expected. Thread-safe via CAS.
     */
    public boolean tryComplete() {
        if (actualCount.get() == expectedCount
                && receivedTradeIds.equals(expectedTradeIds)
                && completed.compareAndSet(false, true)) {
            status = BatchStatus.SUCCESS;
            return true;
        }
        return false;
    }

    public boolean isTimedOut(Instant now) {
        return status == BatchStatus.PENDING && now.isAfter(deadline);
    }

    /** Transitions to FAIL. Returns true only for the first caller. */
    public boolean markFail() {
        if (completed.compareAndSet(false, true)) {
            status = BatchStatus.FAIL;
            return true;
        }
        return false;
    }

    public Collection<ProcessedEvent> drainEvents() {
        return events;
    }

    public Set<String> getMissingTradeIds() {
        Set<String> missing = new HashSet<>(expectedTradeIds);
        missing.removeAll(receivedTradeIds);
        return missing;
    }

    public String getTradeGroupId() { return tradeGroupId; }
    public int getExpectedCount()   { return expectedCount; }
    public int getActualCount()     { return actualCount.get(); }
    public BatchStatus getStatus()  { return status; }
    public Instant getDeadline()    { return deadline; }
}

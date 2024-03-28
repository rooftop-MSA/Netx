package org.rooftop.netx.javasupports;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.assertj.core.api.Assertions;
import org.rooftop.netx.api.SuccessWith;
import org.rooftop.netx.api.TransactionCommitEvent;
import org.rooftop.netx.api.TransactionCommitListener;
import org.rooftop.netx.api.TransactionJoinEvent;
import org.rooftop.netx.api.TransactionJoinListener;
import org.rooftop.netx.api.TransactionRollbackEvent;
import org.rooftop.netx.api.TransactionRollbackListener;
import org.rooftop.netx.api.TransactionStartEvent;
import org.rooftop.netx.api.TransactionStartListener;
import org.rooftop.netx.meta.TransactionHandler;
import reactor.core.publisher.Mono;

@TransactionHandler
public class TransactionEventListeners {

    private final Map<String, Integer> receivedTransactions = new ConcurrentHashMap<>();

    public void clear() {
        receivedTransactions.clear();
    }

    public void assertTransactionCount(String transactionState, int count) {
        Assertions.assertThat(receivedTransactions.getOrDefault(transactionState, 0))
            .isEqualTo(count);
    }

    @TransactionStartListener(
        event = Event.class,
        noRollbackFor = IllegalArgumentException.class,
        successWith = SuccessWith.PUBLISH_JOIN
    )
    public void listenTransactionStartEvent(TransactionStartEvent transactionStartEvent) {
        incrementTransaction("START");
        Event event = transactionStartEvent.decodeEvent(Event.class);
        transactionStartEvent.setNextEvent(event);
    }

    @TransactionJoinListener(
        event = Event.class,
        successWith = SuccessWith.PUBLISH_COMMIT
    )
    public void listenTransactionJoinEvent(TransactionJoinEvent transactionJoinEvent) {
        incrementTransaction("JOIN");
        Event event = transactionJoinEvent.decodeEvent(Event.class);
        transactionJoinEvent.setNextEvent(event);
        if (event.event() < 0) {
            throw new IllegalArgumentException();
        }
    }

    @TransactionCommitListener
    public Mono<Long> listenTransactionCommitEvent(TransactionCommitEvent transactionCommitEvent) {
        incrementTransaction("COMMIT");
        return Mono.just(1L);
    }

    @TransactionRollbackListener(event = Event.class)
    public String listenTransactionRollbackEvent(TransactionRollbackEvent transactionRollbackEvent) {
        incrementTransaction("ROLLBACK");
        transactionRollbackEvent.decodeEvent(Event.class);
        return "listenTransactionRollbackEvent";
    }

    private void incrementTransaction(String transactionState) {
        receivedTransactions.put(transactionState,
            receivedTransactions.getOrDefault(transactionState, 0) + 1);
    }
}

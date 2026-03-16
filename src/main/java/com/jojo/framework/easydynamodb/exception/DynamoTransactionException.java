package com.jojo.framework.easydynamodb.exception;

import java.util.Collections;
import java.util.List;

/**
 * Thrown when a DynamoDB transaction fails.
 * <p>
 * Contains the list of cancellation reasons when the transaction was cancelled
 * (e.g., condition check failures on individual items). Each reason corresponds
 * to one item in the transaction, in order.
 *
 * <pre>{@code
 * try {
 *     ddm.transact()
 *         .put(user)
 *         .update(wallet, w -> w.setBalance(newBalance))
 *         .execute();
 * } catch (DynamoTransactionException e) {
 *     for (var reason : e.getCancellationReasons()) {
 *         System.err.println(reason);
 *     }
 * }
 * }</pre>
 */
public class DynamoTransactionException extends DynamoException {

    private final List<String> cancellationReasons;

    public DynamoTransactionException(String message) {
        super(message);
        this.cancellationReasons = Collections.emptyList();
    }

    public DynamoTransactionException(String message, Throwable cause) {
        super(message, cause);
        this.cancellationReasons = Collections.emptyList();
    }

    public DynamoTransactionException(String message, List<String> cancellationReasons, Throwable cause) {
        super(message, cause);
        this.cancellationReasons = cancellationReasons != null
                ? List.copyOf(cancellationReasons) : Collections.emptyList();
    }

    /**
     * Returns the list of cancellation reasons, one per transaction item (in order).
     * Empty reasons indicate the item was not the cause of the cancellation.
     *
     * @return unmodifiable list of cancellation reason strings
     */
    public List<String> getCancellationReasons() {
        return cancellationReasons;
    }
}

package com.jojo.framework.easydynamodb.exception;

import java.util.Collections;
import java.util.List;

/**
 * Thrown when a DynamoDB transaction fails.
 * 当 DynamoDB 事务失败时抛出。
 * <p>
 * Contains the list of cancellation reasons when the transaction was cancelled
 * (e.g., condition check failures on individual items). Each reason corresponds
 * to one item in the transaction, in order.
 * 包含事务被取消时的取消原因列表（例如，单个项目的条件检查失败）。
 * 每个原因按顺序对应事务中的一个项目。
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

    /**
     * Constructs a new DynamoTransactionException with the specified message.
     * 使用指定的消息构造一个新的 DynamoTransactionException。
     *
     * @param message the detail message / 详细错误信息
     */
    public DynamoTransactionException(String message) {
        super(message);
        this.cancellationReasons = Collections.emptyList();
    }

    /**
     * Constructs a new DynamoTransactionException with the specified message and cause.
     * 使用指定的消息和原因构造一个新的 DynamoTransactionException。
     *
     * @param message the detail message / 详细错误信息
     * @param cause   the underlying cause / 底层原因
     */
    public DynamoTransactionException(String message, Throwable cause) {
        super(message, cause);
        this.cancellationReasons = Collections.emptyList();
    }

    /**
     * Constructs a new DynamoTransactionException with message, cancellation reasons, and cause.
     * 使用消息、取消原因列表和底层原因构造一个新的 DynamoTransactionException。
     *
     * @param message             the detail message / 详细错误信息
     * @param cancellationReasons the list of cancellation reasons per transaction item / 每个事务项目的取消原因列表
     * @param cause               the underlying cause / 底层原因
     */
    public DynamoTransactionException(String message, List<String> cancellationReasons, Throwable cause) {
        super(message, cause);
        this.cancellationReasons = cancellationReasons != null
                ? List.copyOf(cancellationReasons) : Collections.emptyList();
    }

    /**
     * Returns the list of cancellation reasons, one per transaction item (in order).
     * 返回取消原因列表，按事务项目顺序排列，每个项目对应一个原因。
     * <p>
     * Empty reasons indicate the item was not the cause of the cancellation.
     * 空的原因表示该项目不是取消的原因。
     *
     * @return unmodifiable list of cancellation reason strings / 不可变的取消原因字符串列表
     */
    public List<String> getCancellationReasons() {
        return cancellationReasons;
    }
}

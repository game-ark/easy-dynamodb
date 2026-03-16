package com.jojo.framework.easydynamodb.exception;

/**
 * Thrown when a DynamoDB conditional expression check fails.
 * 当 DynamoDB 条件表达式检查失败时抛出。
 * <p>
 * This typically occurs when:
 * 通常在以下情况下发生：
 * <ul>
 *   <li>A condition expression on a put/update/delete evaluates to false / put/update/delete 上的条件表达式评估为 false</li>
 *   <li>A transaction condition check fails / 事务条件检查失败</li>
 *   <li>Optimistic locking detects a concurrent modification / 乐观锁检测到并发修改</li>
 * </ul>
 *
 * <pre>{@code
 * try {
 *     ddm.save(user, c -> c.condition("attribute_not_exists(userId)"));
 * } catch (DynamoConditionFailedException e) {
 *     // Item already exists — handle conflict
 * }
 * }</pre>
 */
public class DynamoConditionFailedException extends DynamoException {

    /**
     * Constructs a new DynamoConditionFailedException with the specified message.
     * 使用指定的消息构造一个新的 DynamoConditionFailedException。
     *
     * @param message the detail message / 详细错误信息
     */
    public DynamoConditionFailedException(String message) {
        super(message);
    }

    /**
     * Constructs a new DynamoConditionFailedException with the specified message and cause.
     * 使用指定的消息和原因构造一个新的 DynamoConditionFailedException。
     *
     * @param message the detail message / 详细错误信息
     * @param cause   the underlying cause / 底层原因
     */
    public DynamoConditionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}

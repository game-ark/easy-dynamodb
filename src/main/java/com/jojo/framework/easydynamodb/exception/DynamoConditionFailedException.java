package com.jojo.framework.easydynamodb.exception;

/**
 * Thrown when a DynamoDB conditional expression check fails.
 * <p>
 * This typically occurs when:
 * <ul>
 *   <li>A condition expression on a put/update/delete evaluates to false</li>
 *   <li>A transaction condition check fails</li>
 *   <li>Optimistic locking detects a concurrent modification</li>
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

    public DynamoConditionFailedException(String message) {
        super(message);
    }

    public DynamoConditionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}

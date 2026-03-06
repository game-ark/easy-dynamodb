package com.jojo.framework.easydynamodb.exception;

/**
 * Base exception for all EasyDynamodb operations.
 * Wraps AWS SDK exceptions and other runtime errors.
 */
public class DynamoException extends RuntimeException {

    public DynamoException(String message) {
        super(message);
    }

    public DynamoException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.jojo.framework.easydynamodb.exception;

/**
 * Exception thrown when entity annotation configuration is invalid.
 * Typically raised during initialization/registration phase.
 */
public class DynamoConfigException extends DynamoException {

    public DynamoConfigException(String message) {
        super(message);
    }
}

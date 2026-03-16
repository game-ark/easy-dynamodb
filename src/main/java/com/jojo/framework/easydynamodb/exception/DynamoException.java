package com.jojo.framework.easydynamodb.exception;

/**
 * Base exception for all EasyDynamodb operations.
 * 所有 EasyDynamodb 操作的基础异常类。
 * <p>
 * Wraps AWS SDK exceptions and other runtime errors.
 * 封装 AWS SDK 异常和其他运行时错误。
 */
public class DynamoException extends RuntimeException {

    /**
     * Constructs a new DynamoException with the specified message.
     * 使用指定的消息构造一个新的 DynamoException。
     *
     * @param message the detail message / 详细错误信息
     */
    public DynamoException(String message) {
        super(message);
    }

    /**
     * Constructs a new DynamoException with the specified message and cause.
     * 使用指定的消息和原因构造一个新的 DynamoException。
     *
     * @param message the detail message / 详细错误信息
     * @param cause   the underlying cause / 底层原因
     */
    public DynamoException(String message, Throwable cause) {
        super(message, cause);
    }
}

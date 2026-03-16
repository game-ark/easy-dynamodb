package com.jojo.framework.easydynamodb.exception;

/**
 * Exception thrown when entity annotation configuration is invalid.
 * 当实体注解配置无效时抛出的异常。
 * <p>
 * Typically raised during initialization/registration phase.
 * 通常在初始化/注册阶段抛出。
 */
public class DynamoConfigException extends DynamoException {

    /**
     * Constructs a new DynamoConfigException with the specified message.
     * 使用指定的消息构造一个新的 DynamoConfigException。
     *
     * @param message the detail message / 详细错误信息
     */
    public DynamoConfigException(String message) {
        super(message);
    }
}

package com.jojo.framework.easydynamodb.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Internal logging facade for EasyDynamodb.
 * EasyDynamodb 的内部日志门面。
 * <p>
 * Wraps SLF4J and respects the log level configured via {@code DDM.Builder.logLevel()}.
 * When logging is disabled ({@code enableLogging(false)}), all log calls are no-ops.
 * 封装 SLF4J 并遵循通过 {@code DDM.Builder.logLevel()} 配置的日志级别。
 * 当日志被禁用时（{@code enableLogging(false)}），所有日志调用均为空操作。
 * <p>
 * This class is a singleton — the log level and enabled state are set once during
 * DDM construction and shared across all operations.
 * 此类为单例——日志级别和启用状态在 DDM 构建时设置一次，并在所有操作间共享。
 */
public final class DdmLogger {

    private static volatile boolean enabled = false;
    private static volatile Level level = Level.INFO;

    private final Logger logger;

    private DdmLogger(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    /**
     * Creates a new DdmLogger instance for the given class.
     * 为给定的类创建新的 DdmLogger 实例。
     *
     * @param clazz the class to create the logger for / 要创建日志记录器的类
     * @return a new DdmLogger instance / 新的 DdmLogger 实例
     */
    public static DdmLogger getLogger(Class<?> clazz) {
        return new DdmLogger(clazz);
    }

    /**
     * Called once during DDM construction to configure global logging state.
     * 在 DDM 构建期间调用一次，用于配置全局日志状态。
     *
     * @param enableLogging whether to enable logging / 是否启用日志
     * @param logLevel      the minimum log level, defaults to INFO if null / 最低日志级别，为 null 时默认为 INFO
     */
    public static void configure(boolean enableLogging, Level logLevel) {
        enabled = enableLogging;
        level = logLevel != null ? logLevel : Level.INFO;
    }

    /**
     * Returns whether logging is currently enabled.
     * 返回当前是否启用了日志。
     *
     * @return true if logging is enabled / 如果日志已启用则返回 true
     */
    public static boolean isEnabled() {
        return enabled;
    }

    /**
     * Logs a message at TRACE level if logging is enabled and the configured level permits.
     * 如果日志已启用且配置的级别允许，则以 TRACE 级别记录消息。
     *
     * @param msg  the log message pattern / 日志消息模式
     * @param args the message arguments / 消息参数
     */
    public void trace(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.TRACE.toInt()) {
            logger.trace(msg, args);
        }
    }

    /**
     * Logs a message at DEBUG level if logging is enabled and the configured level permits.
     * 如果日志已启用且配置的级别允许，则以 DEBUG 级别记录消息。
     *
     * @param msg  the log message pattern / 日志消息模式
     * @param args the message arguments / 消息参数
     */
    public void debug(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.DEBUG.toInt()) {
            logger.debug(msg, args);
        }
    }

    /**
     * Logs a message at INFO level if logging is enabled and the configured level permits.
     * 如果日志已启用且配置的级别允许，则以 INFO 级别记录消息。
     *
     * @param msg  the log message pattern / 日志消息模式
     * @param args the message arguments / 消息参数
     */
    public void info(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.INFO.toInt()) {
            logger.info(msg, args);
        }
    }

    /**
     * Logs a message at WARN level if logging is enabled and the configured level permits.
     * 如果日志已启用且配置的级别允许，则以 WARN 级别记录消息。
     *
     * @param msg  the log message pattern / 日志消息模式
     * @param args the message arguments / 消息参数
     */
    public void warn(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.WARN.toInt()) {
            logger.warn(msg, args);
        }
    }

    /**
     * Logs a message at ERROR level if logging is enabled and the configured level permits.
     * 如果日志已启用且配置的级别允许，则以 ERROR 级别记录消息。
     *
     * @param msg  the log message pattern / 日志消息模式
     * @param args the message arguments / 消息参数
     */
    public void error(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.ERROR.toInt()) {
            logger.error(msg, args);
        }
    }
}

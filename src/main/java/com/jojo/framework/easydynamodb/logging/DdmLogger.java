package com.jojo.framework.easydynamodb.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Internal logging facade for EasyDynamodb.
 * <p>
 * Wraps SLF4J and respects the log level configured via {@code DDM.Builder.logLevel()}.
 * When logging is disabled ({@code enableLogging(false)}), all log calls are no-ops.
 * <p>
 * This class is a singleton — the log level and enabled state are set once during
 * DDM construction and shared across all operations.
 */
public final class DdmLogger {

    private static volatile boolean enabled = false;
    private static volatile Level level = Level.INFO;

    private final Logger logger;

    private DdmLogger(Class<?> clazz) {
        this.logger = LoggerFactory.getLogger(clazz);
    }

    public static DdmLogger getLogger(Class<?> clazz) {
        return new DdmLogger(clazz);
    }

    /**
     * Called once during DDM construction to configure global logging state.
     */
    public static void configure(boolean enableLogging, Level logLevel) {
        enabled = enableLogging;
        level = logLevel != null ? logLevel : Level.INFO;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    public void trace(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.TRACE.toInt()) {
            logger.trace(msg, args);
        }
    }

    public void debug(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.DEBUG.toInt()) {
            logger.debug(msg, args);
        }
    }

    public void info(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.INFO.toInt()) {
            logger.info(msg, args);
        }
    }

    public void warn(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.WARN.toInt()) {
            logger.warn(msg, args);
        }
    }

    public void error(String msg, Object... args) {
        if (enabled && level.toInt() <= Level.ERROR.toInt()) {
            logger.error(msg, args);
        }
    }
}

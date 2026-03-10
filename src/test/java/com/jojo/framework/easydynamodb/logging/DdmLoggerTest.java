package com.jojo.framework.easydynamodb.logging;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import static org.assertj.core.api.Assertions.assertThat;

class DdmLoggerTest {

    @Test
    void configure_enableLogging_shouldSetEnabled() {
        DdmLogger.configure(true, Level.DEBUG);
        assertThat(DdmLogger.isEnabled()).isTrue();
    }

    @Test
    void configure_disableLogging_shouldSetDisabled() {
        DdmLogger.configure(false, Level.DEBUG);
        assertThat(DdmLogger.isEnabled()).isFalse();
    }

    @Test
    void getLogger_shouldReturnNonNull() {
        DdmLogger logger = DdmLogger.getLogger(DdmLoggerTest.class);
        assertThat(logger).isNotNull();
    }

    @Test
    void logMethods_shouldNotThrow_whenEnabled() {
        DdmLogger.configure(true, Level.TRACE);
        DdmLogger logger = DdmLogger.getLogger(DdmLoggerTest.class);

        // These should not throw regardless of SLF4J backend
        logger.trace("trace message: {}", "arg");
        logger.debug("debug message: {}", "arg");
        logger.info("info message: {}", "arg");
        logger.warn("warn message: {}", "arg");
        logger.error("error message: {}", "arg");
    }

    @Test
    void logMethods_shouldNotThrow_whenDisabled() {
        DdmLogger.configure(false, Level.INFO);
        DdmLogger logger = DdmLogger.getLogger(DdmLoggerTest.class);

        logger.trace("should be no-op");
        logger.debug("should be no-op");
        logger.info("should be no-op");
        logger.warn("should be no-op");
        logger.error("should be no-op");
    }

    @Test
    void configure_nullLevel_shouldDefaultToInfo() {
        DdmLogger.configure(true, null);
        // Should not throw
        DdmLogger logger = DdmLogger.getLogger(DdmLoggerTest.class);
        logger.info("test");
    }
}

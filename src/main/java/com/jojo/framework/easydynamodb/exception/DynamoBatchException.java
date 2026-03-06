package com.jojo.framework.easydynamodb.exception;

import java.util.List;

/**
 * Exception thrown when a batch operation has partial failures.
 * Contains the list of individual failures with their keys and error messages.
 */
public class DynamoBatchException extends DynamoException {

    private final List<BatchFailure> failures;

    public DynamoBatchException(List<BatchFailure> failures) {
        super(String.format("Batch operation failed with %d error(s)", failures.size()));
        this.failures = List.copyOf(failures);
    }

    public List<BatchFailure> getFailures() {
        return failures;
    }

    /**
     * Represents a single failure within a batch operation.
     *
     * @param key          the key of the failed item
     * @param errorMessage the error description
     */
    public record BatchFailure(Object key, String errorMessage) {}
}

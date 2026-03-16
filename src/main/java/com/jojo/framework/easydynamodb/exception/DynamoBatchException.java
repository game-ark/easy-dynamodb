package com.jojo.framework.easydynamodb.exception;

import java.util.List;

/**
 * Exception thrown when a batch operation has partial failures.
 * 当批量操作存在部分失败时抛出的异常。
 * <p>
 * Contains the list of individual failures with their keys and error messages.
 * 包含各个失败项的键和错误信息列表。
 */
public class DynamoBatchException extends DynamoException {

    private final List<BatchFailure> failures;

    /**
     * Constructs a new DynamoBatchException with the given list of failures.
     * 使用给定的失败列表构造一个新的 DynamoBatchException。
     *
     * @param failures the list of batch failures / 批量失败列表
     */
    public DynamoBatchException(List<BatchFailure> failures) {
        super(String.format("Batch operation failed with %d error(s)", failures.size()));
        this.failures = List.copyOf(failures);
    }

    /**
     * Returns the list of individual batch failures.
     * 返回各个批量失败项的列表。
     *
     * @return unmodifiable list of batch failures / 不可变的批量失败列表
     */
    public List<BatchFailure> getFailures() {
        return failures;
    }

    /**
     * Represents a single failure within a batch operation.
     * 表示批量操作中的单个失败项。
     *
     * @param key          the key of the failed item / 失败项的键
     * @param errorMessage the error description / 错误描述
     */
    public record BatchFailure(Object key, String errorMessage) {}
}

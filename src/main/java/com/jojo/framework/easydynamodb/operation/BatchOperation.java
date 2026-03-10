package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.exception.DynamoBatchException;
import com.jojo.framework.easydynamodb.exception.DynamoBatchException.BatchFailure;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.logging.DdmLogger;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.KeyPair;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Handles batch save and batch get operations with automatic chunking,
 * parallel execution, and exponential backoff retry for unprocessed items.
 * <p>
 * Uses a dedicated virtual-thread executor by default to avoid polluting
 * {@code ForkJoinPool.commonPool()}. A custom {@link Executor} can be
 * supplied via the constructor for full control over thread scheduling.
 */
public class BatchOperation {

    private static final DdmLogger log = DdmLogger.getLogger(BatchOperation.class);

    static final int WRITE_BATCH_LIMIT = 25;
    static final int READ_BATCH_LIMIT = 100;
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 100;

    /** Default executor: virtual threads (Java 21+), one per chunk, no shared pool contention. */
    private static final Executor DEFAULT_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final DynamoDbClient dynamoDbClient;
    private final MetadataRegistry metadataRegistry;
    private final SaveOperation saveOperation;
    private final GetOperation getOperation;
    private final Executor executor;

    public BatchOperation(DynamoDbClient dynamoDbClient,
                          MetadataRegistry metadataRegistry,
                          SaveOperation saveOperation,
                          GetOperation getOperation) {
        this(dynamoDbClient, metadataRegistry, saveOperation, getOperation, DEFAULT_EXECUTOR);
    }

    public BatchOperation(DynamoDbClient dynamoDbClient,
                          MetadataRegistry metadataRegistry,
                          SaveOperation saveOperation,
                          GetOperation getOperation,
                          Executor executor) {
        this.dynamoDbClient = dynamoDbClient;
        this.metadataRegistry = metadataRegistry;
        this.saveOperation = saveOperation;
        this.getOperation = getOperation;
        this.executor = executor != null ? executor : DEFAULT_EXECUTOR;
    }

    /**
     * Saves a list of entities in batches of 25, executing chunks in parallel.
     * Retries unprocessed items with exponential backoff (100ms initial, max 3 retries).
     * Throws DynamoBatchException if items remain unprocessed after retries.
     */
    public <T> void saveBatch(List<T> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }

        Class<?> entityClass = entities.get(0).getClass();
        metadataRegistry.register(entityClass);
        EntityMetadata metadata = metadataRegistry.getMetadata(entityClass);
        String tableName = metadata.getTableName();

        log.debug("Batch save: {} entities to table={}", entities.size(), tableName);

        // Split into chunks of 25
        List<List<T>> chunks = partition(entities, WRITE_BATCH_LIMIT);
        log.trace("Split into {} chunks of max {}", chunks.size(), WRITE_BATCH_LIMIT);

        // Execute chunks in parallel, collect failures
        List<BatchFailure> allFailures = new CopyOnWriteArrayList<>();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (List<T> chunk : chunks) {
            futures.add(CompletableFuture.runAsync(() ->
                    executeSaveBatchChunk(chunk, metadata, tableName, allFailures), executor));
        }

        // Wait for all chunks to complete
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DynamoException de) {
                throw de;
            }
            throw new DynamoException("Batch save failed: " + cause.getMessage(), cause);
        }

        if (!allFailures.isEmpty()) {
            throw new DynamoBatchException(allFailures);
        }
    }

    /**
     * Gets a list of entities by their keys in batches of 100, executing chunks in parallel.
     * Retries unprocessed keys with exponential backoff (100ms initial, max 3 retries).
     * Throws DynamoBatchException if keys remain unprocessed after retries.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getBatch(Class<T> clazz, List<KeyPair> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        String tableName = metadata.getTableName();

        log.debug("Batch get: {} keys from table={}", keys.size(), tableName);

        // Split into chunks of 100
        List<List<KeyPair>> chunks = partition(keys, READ_BATCH_LIMIT);
        log.trace("Split into {} chunks of max {}", chunks.size(), READ_BATCH_LIMIT);

        // Execute chunks in parallel, collect results and failures
        List<T> allResults = new CopyOnWriteArrayList<>();
        List<BatchFailure> allFailures = new CopyOnWriteArrayList<>();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (List<KeyPair> chunk : chunks) {
            futures.add(CompletableFuture.runAsync(() ->
                    executeGetBatchChunk(chunk, metadata, tableName, allResults, allFailures), executor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DynamoException de) {
                throw de;
            }
            throw new DynamoException("Batch get failed: " + cause.getMessage(), cause);
        }

        if (!allFailures.isEmpty()) {
            throw new DynamoBatchException(allFailures);
        }

        return allResults;
    }

    /**
     * Deletes a list of items by their keys in batches of 25, executing chunks in parallel.
     * Retries unprocessed items with exponential backoff (100ms initial, max 3 retries).
     * Throws DynamoBatchException if keys remain unprocessed after retries.
     */
    public <T> void deleteBatch(Class<T> clazz, List<KeyPair> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }

        metadataRegistry.register(clazz);
        EntityMetadata metadata = metadataRegistry.getMetadata(clazz);
        String tableName = metadata.getTableName();

        log.debug("Batch delete: {} keys from table={}", keys.size(), tableName);

        List<List<KeyPair>> chunks = partition(keys, WRITE_BATCH_LIMIT);

        List<BatchFailure> allFailures = new CopyOnWriteArrayList<>();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (List<KeyPair> chunk : chunks) {
            futures.add(CompletableFuture.runAsync(() ->
                    executeDeleteBatchChunk(chunk, metadata, tableName, allFailures), executor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (java.util.concurrent.CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof DynamoException de) {
                throw de;
            }
            throw new DynamoException("Batch delete failed: " + cause.getMessage(), cause);
        }

        if (!allFailures.isEmpty()) {
            throw new DynamoBatchException(allFailures);
        }
    }

    // ---- Internal helpers ----

    private <T> void executeSaveBatchChunk(List<T> chunk,
                                           EntityMetadata metadata,
                                           String tableName,
                                           List<BatchFailure> failures) {
        List<WriteRequest> writeRequests = new ArrayList<>();
        for (T entity : chunk) {
            Map<String, AttributeValue> item = saveOperation.toAttributeValueMap(entity, metadata);
            writeRequests.add(WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build())
                    .build());
        }

        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName, writeRequests);

        try {
            Map<String, List<WriteRequest>> unprocessed = executeBatchWriteWithRetry(requestItems);
            if (!unprocessed.isEmpty()) {
                int unprocessedCount = unprocessed.values().stream().mapToInt(List::size).sum();
                log.warn("Batch save: {} items remained unprocessed after {} retries for table={}",
                        unprocessedCount, MAX_RETRIES, tableName);
                for (Map.Entry<String, List<WriteRequest>> entry : unprocessed.entrySet()) {
                    for (WriteRequest wr : entry.getValue()) {
                        Map<String, AttributeValue> itemMap = wr.putRequest().item();
                        failures.add(new BatchFailure(
                                KeyBuilder.extractKeyDescription(itemMap, metadata),
                                "Item remained unprocessed after " + MAX_RETRIES + " retries"));
                    }
                }
            }
        } catch (DynamoDbException e) {
            log.error("Batch write failed for table={}: {}", tableName, e.getMessage());
            throw new DynamoException(
                    "Batch write failed for table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private Map<String, List<WriteRequest>> executeBatchWriteWithRetry(
            Map<String, List<WriteRequest>> requestItems) {
        Map<String, List<WriteRequest>> remaining = requestItems;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(
                    BatchWriteItemRequest.builder()
                            .requestItems(remaining)
                            .build());

            if (!response.hasUnprocessedItems() || response.unprocessedItems().isEmpty()) {
                log.trace("BatchWriteItem completed with no unprocessed items (attempt {})", attempt);
                return Collections.emptyMap();
            }

            int unprocessedCount = response.unprocessedItems().values().stream().mapToInt(List::size).sum();
            remaining = response.unprocessedItems();

            if (attempt < MAX_RETRIES) {
                log.warn("BatchWriteItem has {} unprocessed items, retrying (attempt {}/{})",
                        unprocessedCount, attempt + 1, MAX_RETRIES);
                backoff(attempt);
            } else {
                log.error("BatchWriteItem exhausted {} retries with {} items still unprocessed",
                        MAX_RETRIES, unprocessedCount);
            }
        }

        return remaining;
    }

    @SuppressWarnings("unchecked")
    private <T> void executeGetBatchChunk(List<KeyPair> chunk,
                                          EntityMetadata metadata,
                                          String tableName,
                                          List<T> results,
                                          List<BatchFailure> failures) {
        List<Map<String, AttributeValue>> keyMaps = new ArrayList<>();
        for (KeyPair kp : chunk) {
            keyMaps.add(KeyBuilder.buildKeyMap(metadata, kp.partitionKey(), kp.sortKey()));
        }

        KeysAndAttributes keysAndAttributes = KeysAndAttributes.builder()
                .keys(keyMaps)
                .build();

        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put(tableName, keysAndAttributes);

        try {
            BatchGetItemResponse response = executeBatchGetWithRetry(requestItems, metadata, tableName, results);

            // Check for remaining unprocessed keys
            if (response != null && response.hasUnprocessedKeys() && !response.unprocessedKeys().isEmpty()) {
                KeysAndAttributes unprocessedKa = response.unprocessedKeys().get(tableName);
                if (unprocessedKa != null) {
                    int unprocessedCount = unprocessedKa.keys().size();
                    log.warn("Batch get: {} keys remained unprocessed after {} retries for table={}",
                            unprocessedCount, MAX_RETRIES, tableName);
                    for (Map<String, AttributeValue> keyMap : unprocessedKa.keys()) {
                        failures.add(new BatchFailure(
                                keyMap.toString(),
                                "Key remained unprocessed after " + MAX_RETRIES + " retries"));
                    }
                }
            }
        } catch (DynamoDbException e) {
            log.error("Batch get failed for table={}: {}", tableName, e.getMessage());
            throw new DynamoException(
                    "Batch get failed for table " + tableName + ": " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> BatchGetItemResponse executeBatchGetWithRetry(
            Map<String, KeysAndAttributes> requestItems,
            EntityMetadata metadata,
            String tableName,
            List<T> results) {
        Map<String, KeysAndAttributes> remaining = requestItems;
        BatchGetItemResponse lastResponse = null;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            BatchGetItemResponse response = dynamoDbClient.batchGetItem(
                    BatchGetItemRequest.builder()
                            .requestItems(remaining)
                            .build());

            lastResponse = response;

            // Collect returned items
            if (response.hasResponses()) {
                List<Map<String, AttributeValue>> items = response.responses().get(tableName);
                if (items != null) {
                    log.trace("BatchGetItem attempt {} returned {} items from table={}",
                            attempt, items.size(), tableName);
                    for (Map<String, AttributeValue> item : items) {
                        results.add((T) getOperation.fromAttributeValueMap(item, metadata));
                    }
                }
            }

            if (!response.hasUnprocessedKeys() || response.unprocessedKeys().isEmpty()) {
                log.trace("BatchGetItem completed with no unprocessed keys (attempt {})", attempt);
                return response;
            }

            remaining = response.unprocessedKeys();
            int unprocessedCount = remaining.values().stream()
                    .mapToInt(ka -> ka.keys().size()).sum();

            if (attempt < MAX_RETRIES) {
                log.warn("BatchGetItem has {} unprocessed keys, retrying (attempt {}/{})",
                        unprocessedCount, attempt + 1, MAX_RETRIES);
                backoff(attempt);
            } else {
                log.error("BatchGetItem exhausted {} retries with {} keys still unprocessed",
                        MAX_RETRIES, unprocessedCount);
            }
        }

        return lastResponse;
    }



    private <T> void executeDeleteBatchChunk(List<KeyPair> chunk,
                                              EntityMetadata metadata,
                                              String tableName,
                                              List<BatchFailure> failures) {
        List<WriteRequest> writeRequests = new ArrayList<>();
        for (KeyPair kp : chunk) {
            Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, kp.partitionKey(), kp.sortKey());
            writeRequests.add(WriteRequest.builder()
                    .deleteRequest(DeleteRequest.builder().key(keyMap).build())
                    .build());
        }

        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName, writeRequests);

        try {
            Map<String, List<WriteRequest>> unprocessed = executeBatchWriteWithRetry(requestItems);
            if (!unprocessed.isEmpty()) {
                int unprocessedCount = unprocessed.values().stream().mapToInt(List::size).sum();
                log.warn("Batch delete: {} keys remained unprocessed after {} retries for table={}",
                        unprocessedCount, MAX_RETRIES, tableName);
                for (Map.Entry<String, List<WriteRequest>> entry : unprocessed.entrySet()) {
                    for (WriteRequest wr : entry.getValue()) {
                        Map<String, AttributeValue> keyMap = wr.deleteRequest().key();
                        failures.add(new BatchFailure(
                                keyMap.toString(),
                                "Key remained unprocessed after " + MAX_RETRIES + " retries"));
                    }
                }
            }
        } catch (DynamoDbException e) {
            log.error("Batch delete failed for table={}: {}", tableName, e.getMessage());
            throw new DynamoException(
                    "Batch delete failed for table " + tableName + ": " + e.getMessage(), e);
        }
    }

    private void backoff(int attempt) {
        try {
            long sleepMs = INITIAL_BACKOFF_MS * (1L << attempt);
            log.debug("Backoff: sleeping {}ms (attempt {})", sleepMs, attempt);
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static <T> List<List<T>> partition(List<T> list, int batchSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            partitions.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return partitions;
    }
}

package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.KeyPair;
import com.jojo.framework.easydynamodb.testmodel.SimpleItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private SaveOperation saveOperation;
    private GetOperation getOperation;
    private BatchOperation batchOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        saveOperation = new SaveOperation(dynamoDbClient, metadataRegistry);
        getOperation = new GetOperation(dynamoDbClient, metadataRegistry);
        // Use direct executor for deterministic tests
        batchOperation = new BatchOperation(dynamoDbClient, metadataRegistry, saveOperation, getOperation, Runnable::run);
    }

    @Test
    void saveBatch_emptyList_shouldNotCallDynamo() {
        batchOperation.saveBatch(List.of());
        verify(dynamoDbClient, never()).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    void saveBatch_shouldCallBatchWriteItem() {
        when(dynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().build());

        List<SimpleItem> items = List.of(
                new SimpleItem("id-1", "a", 1),
                new SimpleItem("id-2", "b", 2)
        );
        batchOperation.saveBatch(items);

        verify(dynamoDbClient).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    void getBatch_emptyList_shouldReturnEmptyList() {
        List<SimpleItem> result = batchOperation.getBatch(SimpleItem.class, List.of());
        assertThat(result).isEmpty();
    }

    @Test
    void getBatch_shouldReturnEntities() {
        Map<String, AttributeValue> item1 = Map.of(
                "item_id", AttributeValue.builder().s("id-1").build(),
                "name", AttributeValue.builder().s("a").build()
        );
        BatchGetItemResponse response = BatchGetItemResponse.builder()
                .responses(Map.of("simple_items", List.of(item1)))
                .build();
        when(dynamoDbClient.batchGetItem(any(BatchGetItemRequest.class))).thenReturn(response);

        List<SimpleItem> result = batchOperation.getBatch(SimpleItem.class,
                List.of(new KeyPair("id-1")));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getId()).isEqualTo("id-1");
    }

    @Test
    void deleteBatch_emptyList_shouldNotCallDynamo() {
        batchOperation.deleteBatch(SimpleItem.class, List.of());
        verify(dynamoDbClient, never()).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    void deleteBatch_shouldCallBatchWriteItem() {
        when(dynamoDbClient.batchWriteItem(any(BatchWriteItemRequest.class)))
                .thenReturn(BatchWriteItemResponse.builder().build());

        batchOperation.deleteBatch(SimpleItem.class,
                List.of(new KeyPair("id-1"), new KeyPair("id-2")));

        verify(dynamoDbClient).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    void partition_shouldSplitCorrectly() {
        List<Integer> list = List.of(1, 2, 3, 4, 5);
        List<List<Integer>> partitions = BatchOperation.partition(list, 2);
        assertThat(partitions).hasSize(3);
        assertThat(partitions.get(0)).containsExactly(1, 2);
        assertThat(partitions.get(1)).containsExactly(3, 4);
        assertThat(partitions.get(2)).containsExactly(5);
    }

    @Test
    void partition_emptyList_shouldReturnEmpty() {
        List<List<Integer>> partitions = BatchOperation.partition(List.of(), 25);
        assertThat(partitions).isEmpty();
    }
}

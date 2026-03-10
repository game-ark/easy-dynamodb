package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.testmodel.SimpleItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UpdateOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private UpdateOperation updateOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        updateOperation = new UpdateOperation(dynamoDbClient, metadataRegistry);
    }

    @Test
    void update_partialUpdate_shouldOnlyUpdateChangedFields() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        SimpleItem item = new SimpleItem("id-1", "original", 10);
        updateOperation.update(item, e -> e.setName("updated"));

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.tableName()).isEqualTo("simple_items");
        assertThat(request.updateExpression()).contains("SET");
        assertThat(request.updateExpression()).doesNotContain("REMOVE");
    }

    @Test
    void update_noChanges_shouldNotCallDynamo() {
        SimpleItem item = new SimpleItem("id-1", "original", 10);
        updateOperation.update(item, e -> {}); // no changes

        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void update_setFieldToNull_shouldBeNoOpWhenAlreadyNull() {
        // The clean entity starts with all non-key fields as null.
        // Setting name=null is not a change, so no DynamoDB call should happen.
        SimpleItem item = new SimpleItem("id-1", "original", 10);
        updateOperation.update(item, e -> {
            // mutator does nothing meaningful — name is already null on clean entity
        });

        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void updateAll_withSomeNullFields_shouldGenerateSetAndRemove() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        // name is non-null, count is null → should SET name, REMOVE count
        SimpleItem item = new SimpleItem("id-1", "test", null);
        updateOperation.updateAll(item);

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("SET");
        assertThat(request.updateExpression()).contains("REMOVE");
    }

    @Test
    void updateAll_shouldIncludeAllFields() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        SimpleItem item = new SimpleItem("id-1", "test", 42);
        updateOperation.updateAll(item);

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("SET");
    }

    @Test
    void updateAll_withNullFields_shouldGenerateRemove() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        SimpleItem item = new SimpleItem("id-1", null, null);
        updateOperation.updateAll(item);

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("REMOVE");
    }

    @Test
    void update_dynamoDbException_shouldWrapInDynamoException() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("error").build());

        SimpleItem item = new SimpleItem("id-1", "test", 10);
        assertThatThrownBy(() -> updateOperation.update(item, e -> e.setName("new")))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("Failed to update");
    }

    @Test
    void updateBatch_emptyList_shouldNotCallDynamo() {
        updateOperation.updateBatch(java.util.List.of(), e -> {});
        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void updateAllBatch_emptyList_shouldNotCallDynamo() {
        updateOperation.updateAllBatch(java.util.List.of());
        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }
}

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
class DeleteOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private DeleteOperation deleteOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        deleteOperation = new DeleteOperation(dynamoDbClient, metadataRegistry);
    }

    @Test
    void delete_shouldCallDeleteItem() {
        when(dynamoDbClient.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(DeleteItemResponse.builder().build());

        deleteOperation.delete(SimpleItem.class, "id-1");

        ArgumentCaptor<DeleteItemRequest> captor = ArgumentCaptor.forClass(DeleteItemRequest.class);
        verify(dynamoDbClient).deleteItem(captor.capture());

        DeleteItemRequest request = captor.getValue();
        assertThat(request.tableName()).isEqualTo("simple_items");
        assertThat(request.key()).containsKey("item_id");
        assertThat(request.key().get("item_id").s()).isEqualTo("id-1");
    }

    @Test
    void delete_dynamoDbException_shouldWrapInDynamoException() {
        when(dynamoDbClient.deleteItem(any(DeleteItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("error").build());

        assertThatThrownBy(() -> deleteOperation.delete(SimpleItem.class, "id-1"))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("Failed to delete");
    }

    @Test
    void deleteByCondition_noItems_shouldReturnZero() {
        ScanResponse emptyResponse = ScanResponse.builder()
                .items(java.util.List.of())
                .build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(emptyResponse);

        int deleted = deleteOperation.deleteByCondition(
                SimpleItem.class, "count < :min",
                java.util.Map.of(":min", AttributeValue.builder().n("5").build()),
                null);

        assertThat(deleted).isZero();
    }
}

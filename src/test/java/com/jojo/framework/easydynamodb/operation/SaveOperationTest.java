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
class SaveOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private SaveOperation saveOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        saveOperation = new SaveOperation(dynamoDbClient, metadataRegistry);
    }

    @Test
    void save_shouldCallPutItem() {
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenReturn(PutItemResponse.builder().build());

        SimpleItem item = new SimpleItem("id-1", "test", 10);
        saveOperation.save(item);

        ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(captor.capture());

        PutItemRequest request = captor.getValue();
        assertThat(request.tableName()).isEqualTo("simple_items");
        assertThat(request.item()).containsKey("item_id");
        assertThat(request.item().get("item_id").s()).isEqualTo("id-1");
    }

    @Test
    void save_shouldSkipNullFields() {
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenReturn(PutItemResponse.builder().build());

        SimpleItem item = new SimpleItem("id-1", null, null);
        saveOperation.save(item);

        ArgumentCaptor<PutItemRequest> captor = ArgumentCaptor.forClass(PutItemRequest.class);
        verify(dynamoDbClient).putItem(captor.capture());

        PutItemRequest request = captor.getValue();
        assertThat(request.item()).containsKey("item_id");
        assertThat(request.item()).doesNotContainKey("name");
        assertThat(request.item()).doesNotContainKey("count");
    }

    @Test
    void save_dynamoDbException_shouldWrapInDynamoException() {
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("throttled").build());

        SimpleItem item = new SimpleItem("id-1", "test", 10);
        assertThatThrownBy(() -> saveOperation.save(item))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("Failed to save");
    }

    @Test
    void save_tableNotFound_noAutoCreate_shouldThrow() {
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("not found").build());

        SimpleItem item = new SimpleItem("id-1", "test", 10);
        assertThatThrownBy(() -> saveOperation.save(item))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void toAttributeValueMap_shouldConvertAllNonNullFields() {
        metadataRegistry.register(SimpleItem.class);
        var metadata = metadataRegistry.getMetadata(SimpleItem.class);

        SimpleItem item = new SimpleItem("id-1", "test", 42);
        var map = saveOperation.toAttributeValueMap(item, metadata);

        assertThat(map).containsKey("item_id");
        assertThat(map).containsKey("name");
        assertThat(map).containsKey("count");
        assertThat(map.get("count").n()).isEqualTo("42");
    }
}

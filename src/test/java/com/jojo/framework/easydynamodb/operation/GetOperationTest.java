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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GetOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private GetOperation getOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        getOperation = new GetOperation(dynamoDbClient, metadataRegistry);
    }

    @Test
    void get_shouldReturnEntity() {
        Map<String, AttributeValue> item = Map.of(
                "item_id", AttributeValue.builder().s("id-1").build(),
                "name", AttributeValue.builder().s("test").build(),
                "count", AttributeValue.builder().n("42").build()
        );
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        SimpleItem result = getOperation.get(SimpleItem.class, "id-1");

        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo("id-1");
        assertThat(result.getName()).isEqualTo("test");
        assertThat(result.getCount()).isEqualTo(42);
    }

    @Test
    void get_notFound_shouldReturnNull() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        SimpleItem result = getOperation.get(SimpleItem.class, "nonexistent");
        assertThat(result).isNull();
    }

    @Test
    void get_emptyItem_shouldReturnNull() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(Map.of()).build());

        SimpleItem result = getOperation.get(SimpleItem.class, "nonexistent");
        assertThat(result).isNull();
    }

    @Test
    void get_shouldBuildCorrectKey() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        getOperation.get(SimpleItem.class, "id-1");

        ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(dynamoDbClient).getItem(captor.capture());

        GetItemRequest request = captor.getValue();
        assertThat(request.tableName()).isEqualTo("simple_items");
        assertThat(request.key()).containsKey("item_id");
        assertThat(request.key().get("item_id").s()).isEqualTo("id-1");
    }

    @Test
    void get_consistentRead_shouldSetOnRequest() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        getOperation.get(SimpleItem.class, "id-1", null, true);

        ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(dynamoDbClient).getItem(captor.capture());
        assertThat(captor.getValue().consistentRead()).isTrue();
    }

    @Test
    void get_defaultConsistentRead_shouldBeFalse() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        getOperation.get(SimpleItem.class, "id-1");

        ArgumentCaptor<GetItemRequest> captor = ArgumentCaptor.forClass(GetItemRequest.class);
        verify(dynamoDbClient).getItem(captor.capture());
        assertThat(captor.getValue().consistentRead()).isFalse();
    }

    @Test
    void get_dynamoDbException_shouldWrapInDynamoException() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("error").build());

        assertThatThrownBy(() -> getOperation.get(SimpleItem.class, "id-1"))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("Failed to get");
    }

    @Test
    void fromAttributeValueMap_shouldConvertCorrectly() {
        metadataRegistry.register(SimpleItem.class);
        var metadata = metadataRegistry.getMetadata(SimpleItem.class);

        Map<String, AttributeValue> item = Map.of(
                "item_id", AttributeValue.builder().s("id-1").build(),
                "name", AttributeValue.builder().s("hello").build()
        );

        Object result = getOperation.fromAttributeValueMap(item, metadata);
        assertThat(result).isInstanceOf(SimpleItem.class);
        assertThat(((SimpleItem) result).getId()).isEqualTo("id-1");
        assertThat(((SimpleItem) result).getName()).isEqualTo("hello");
    }
}

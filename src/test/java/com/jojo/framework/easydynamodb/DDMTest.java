package com.jojo.framework.easydynamodb;

import com.jojo.framework.easydynamodb.testmodel.SimpleItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DDMTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Test
    void create_nullClient_shouldThrow() {
        assertThatThrownBy(() -> DDM.create(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builder_nullClient_shouldThrow() {
        assertThatThrownBy(() -> DDM.builder(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void builder_shouldBuildSuccessfully() {
        DDM ddm = DDM.builder(dynamoDbClient).build();
        assertThat(ddm).isNotNull();
    }

    @Test
    void builder_withLogging_shouldBuildSuccessfully() {
        DDM ddm = DDM.builder(dynamoDbClient)
                .enableLogging(true)
                .logLevel(Level.DEBUG)
                .build();
        assertThat(ddm).isNotNull();
    }

    @Test
    void builder_withAllOptions_shouldBuildSuccessfully() {
        DDM ddm = DDM.builder(dynamoDbClient)
                .tablePrefix("test_")
                .autoCreateTable(true)
                .enableLogging(true)
                .logLevel(Level.TRACE)
                .register(SimpleItem.class)
                .build();
        assertThat(ddm).isNotNull();
    }

    @Test
    void save_shouldDelegateToSaveOperation() {
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenReturn(PutItemResponse.builder().build());

        DDM ddm = DDM.create(dynamoDbClient);
        ddm.save(new SimpleItem("id-1", "test", 10));

        verify(dynamoDbClient).putItem(any(PutItemRequest.class));
    }

    @Test
    void get_shouldDelegateToGetOperation() {
        Map<String, AttributeValue> item = Map.of(
                "item_id", AttributeValue.builder().s("id-1").build(),
                "name", AttributeValue.builder().s("test").build()
        );
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().item(item).build());

        DDM ddm = DDM.create(dynamoDbClient);
        SimpleItem result = ddm.get(SimpleItem.class, "id-1");

        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo("id-1");
    }

    @Test
    void get_notFound_shouldReturnNull() {
        when(dynamoDbClient.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder().build());

        DDM ddm = DDM.create(dynamoDbClient);
        SimpleItem result = ddm.get(SimpleItem.class, "nonexistent");

        assertThat(result).isNull();
    }

    @Test
    void delete_shouldDelegateToDeleteOperation() {
        when(dynamoDbClient.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(DeleteItemResponse.builder().build());

        DDM ddm = DDM.create(dynamoDbClient);
        ddm.delete(SimpleItem.class, "id-1");

        verify(dynamoDbClient).deleteItem(any(DeleteItemRequest.class));
    }

    @Test
    void update_shouldDelegateToUpdateOperation() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        DDM ddm = DDM.create(dynamoDbClient);
        SimpleItem item = new SimpleItem("id-1", "test", 10);
        ddm.update(item, e -> e.setName("updated"));

        verify(dynamoDbClient).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void register_shouldNotThrow() {
        DDM ddm = DDM.create(dynamoDbClient);
        ddm.register(SimpleItem.class);
        // Should not throw on re-register
        ddm.register(SimpleItem.class);
    }
}

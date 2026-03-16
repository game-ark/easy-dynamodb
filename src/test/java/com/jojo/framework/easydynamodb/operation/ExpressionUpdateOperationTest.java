package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.model.ConditionExpression;
import com.jojo.framework.easydynamodb.testmodel.GameItem;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExpressionUpdateOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private ExpressionUpdateOperation expressionUpdateOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        expressionUpdateOperation = new ExpressionUpdateOperation(dynamoDbClient, metadataRegistry);
    }

    @Test
    void increment_shouldGenerateCorrectSetExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .increment("count", 10)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.tableName()).isEqualTo("simple_items");
        assertThat(request.updateExpression()).startsWith("SET");
        assertThat(request.updateExpression()).contains(" + ");
        assertThat(request.key()).containsKey("item_id");
        assertThat(request.key().get("item_id").s()).isEqualTo("id-1");
    }

    @Test
    void decrement_shouldGenerateCorrectSetExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .decrement("count", 5)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("SET");
        assertThat(request.updateExpression()).contains(" - ");
    }

    @Test
    void set_attributeNameAndValue_shouldGenerateSetExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .set("name", "newName")
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).startsWith("SET");
        // Should have expression names and values
        assertThat(request.expressionAttributeNames()).isNotEmpty();
        assertThat(request.expressionAttributeValues()).isNotEmpty();
    }

    @Test
    void remove_shouldGenerateRemoveExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .remove("tempFlag", "oldField")
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("REMOVE");
    }

    @Test
    void add_shouldGenerateAddExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .add("viewCount", 1)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("ADD");
    }

    @Test
    void combinedSetAndRemove_shouldGenerateBothClauses() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .increment("count", 10)
                .remove("tempFlag")
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("SET");
        assertThat(request.updateExpression()).contains("REMOVE");
    }

    @Test
    void withCondition_shouldIncludeConditionExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        ConditionExpression cond = ConditionExpression.builder()
                .expression("#count >= :min")
                .name("#count", "count")
                .value(":min", 0)
                .build();

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .increment("count", 10)
                .condition(cond)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.conditionExpression()).isEqualTo("#count >= :min");
        assertThat(request.expressionAttributeNames()).containsEntry("#count", "count");
        assertThat(request.expressionAttributeValues()).containsKey(":min");
    }

    @Test
    void withStringCondition_shouldWork() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .set("name", "test")
                .condition("attribute_exists(item_id)")
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        assertThat(captor.getValue().conditionExpression()).isEqualTo("attribute_exists(item_id)");
    }

    @Test
    void conditionCheckFailed_shouldThrowDynamoConditionFailedException() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder().message("condition failed").build());

        assertThatThrownBy(() ->
                expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                        .increment("count", 10)
                        .condition("attribute_exists(item_id)")
                        .execute()
        ).isInstanceOf(DynamoConditionFailedException.class)
                .hasMessageContaining("Condition check failed");
    }

    @Test
    void dynamoDbException_shouldWrapInDynamoException() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(DynamoDbException.builder().message("error").build());

        assertThatThrownBy(() ->
                expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                        .set("name", "test")
                        .execute()
        ).isInstanceOf(DynamoException.class)
                .hasMessageContaining("Failed to execute expression update");
    }

    @Test
    void noClauses_shouldThrowDynamoException() {
        assertThatThrownBy(() ->
                expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                        .execute()
        ).isInstanceOf(DynamoException.class)
                .hasMessageContaining("at least one SET, REMOVE, ADD, or DELETE");
    }

    @Test
    void withCompositeKey_shouldIncludeSortKey() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(GameItem.class, "game-1", "Zelda")
                .set("rating", 9.5)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.tableName()).isEqualTo("games");
        assertThat(request.key()).containsKey("game_id");
        assertThat(request.key()).containsKey("title");
    }

    @Test
    void incrementIfNotExists_shouldGenerateIfNotExistsExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .incrementIfNotExists("count", 0, 1)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).contains("if_not_exists");
    }

    @Test
    void returnValues_shouldBeIncludedInRequest() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .set("name", "test")
                .returnValues(ReturnValue.ALL_NEW)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        assertThat(captor.getValue().returnValues()).isEqualTo(ReturnValue.ALL_NEW);
    }

    @Test
    void rawSetExpression_shouldBePassedThrough() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .set("#n = :v")
                .name("#n", "name")
                .value(":v", "hello")
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        UpdateItemRequest request = captor.getValue();
        assertThat(request.updateExpression()).isEqualTo("SET #n = :v");
        assertThat(request.expressionAttributeNames()).containsEntry("#n", "name");
    }

    @Test
    void deleteFromSet_shouldGenerateDeleteExpression() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        AttributeValue setToRemove = AttributeValue.builder().ss("tag1", "tag2").build();
        expressionUpdateOperation.expressionUpdate(SimpleItem.class, "id-1")
                .deleteFromSet("tags", setToRemove)
                .execute();

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());

        assertThat(captor.getValue().updateExpression()).contains("DELETE");
    }
}

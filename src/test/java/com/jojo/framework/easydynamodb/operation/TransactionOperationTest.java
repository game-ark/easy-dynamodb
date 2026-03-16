package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.exception.DynamoConditionFailedException;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.exception.DynamoTransactionException;
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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private SaveOperation saveOperation;
    private TransactionOperation transactionOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        saveOperation = new SaveOperation(dynamoDbClient, metadataRegistry);
        transactionOperation = new TransactionOperation(dynamoDbClient, metadataRegistry, saveOperation);
    }

    // ======== TransactWriteItems Tests ========

    @Test
    void transact_put_shouldSendTransactWriteItems() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        SimpleItem item = new SimpleItem("id-1", "test", 10);
        transactionOperation.transact()
                .put(item)
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        TransactWriteItemsRequest request = captor.getValue();
        assertThat(request.transactItems()).hasSize(1);
        assertThat(request.transactItems().get(0).put()).isNotNull();
        assertThat(request.transactItems().get(0).put().tableName()).isEqualTo("simple_items");
    }

    @Test
    void transact_putWithCondition_shouldIncludeConditionExpression() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        SimpleItem item = new SimpleItem("id-1", "test", 10);
        ConditionExpression cond = ConditionExpression.of("attribute_not_exists(item_id)");

        transactionOperation.transact()
                .put(item, cond)
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        Put put = captor.getValue().transactItems().get(0).put();
        assertThat(put.conditionExpression()).isEqualTo("attribute_not_exists(item_id)");
    }

    @Test
    void transact_update_shouldBuildUpdateExpression() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        transactionOperation.transact()
                .update(SimpleItem.class, "id-1", u -> u
                        .increment("count", 10)
                        .condition("attribute_exists(item_id)"))
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        Update update = captor.getValue().transactItems().get(0).update();
        assertThat(update.tableName()).isEqualTo("simple_items");
        assertThat(update.updateExpression()).contains("SET");
        assertThat(update.updateExpression()).contains(" + ");
        assertThat(update.conditionExpression()).isEqualTo("attribute_exists(item_id)");
    }

    @Test
    void transact_delete_shouldBuildDeleteRequest() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        transactionOperation.transact()
                .delete(SimpleItem.class, "id-1")
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        Delete delete = captor.getValue().transactItems().get(0).delete();
        assertThat(delete.tableName()).isEqualTo("simple_items");
        assertThat(delete.key()).containsKey("item_id");
    }

    @Test
    void transact_deleteWithCondition_shouldIncludeCondition() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        ConditionExpression cond = ConditionExpression.builder()
                .expression("#s = :expected")
                .name("#s", "status")
                .value(":expected", "INACTIVE")
                .build();

        transactionOperation.transact()
                .delete(SimpleItem.class, "id-1", null, cond)
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        Delete delete = captor.getValue().transactItems().get(0).delete();
        assertThat(delete.conditionExpression()).isEqualTo("#s = :expected");
    }

    @Test
    void transact_conditionCheck_shouldBuildConditionCheckRequest() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        ConditionExpression cond = ConditionExpression.builder()
                .expression("#count >= :required")
                .name("#count", "count")
                .value(":required", 1)
                .build();

        transactionOperation.transact()
                .put(new SimpleItem("id-1", "test", 10))
                .conditionCheck(SimpleItem.class, "id-2", cond)
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        assertThat(captor.getValue().transactItems()).hasSize(2);
        ConditionCheck check = captor.getValue().transactItems().get(1).conditionCheck();
        assertThat(check.conditionExpression()).isEqualTo("#count >= :required");
        assertThat(check.expressionAttributeNames()).containsEntry("#count", "count");
    }

    @Test
    void transact_conditionCheckWithNullCondition_shouldThrow() {
        assertThatThrownBy(() ->
                transactionOperation.transact()
                        .conditionCheck(SimpleItem.class, "id-1", null)
        ).isInstanceOf(DynamoException.class)
                .hasMessageContaining("non-null condition");
    }

    @Test
    void transact_multipleActions_shouldCombineAll() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        transactionOperation.transact()
                .put(new SimpleItem("id-1", "test", 10))
                .update(SimpleItem.class, "id-2", u -> u.set("name", "updated"))
                .delete(SimpleItem.class, "id-3")
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        assertThat(captor.getValue().transactItems()).hasSize(3);
        assertThat(captor.getValue().transactItems().get(0).put()).isNotNull();
        assertThat(captor.getValue().transactItems().get(1).update()).isNotNull();
        assertThat(captor.getValue().transactItems().get(2).delete()).isNotNull();
    }

    @Test
    void transact_withIdempotencyToken_shouldIncludeToken() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        transactionOperation.transact()
                .put(new SimpleItem("id-1", "test", 10))
                .idempotencyToken("my-token-123")
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        assertThat(captor.getValue().clientRequestToken()).isEqualTo("my-token-123");
    }

    @Test
    void transact_emptyTransaction_shouldThrow() {
        assertThatThrownBy(() -> transactionOperation.transact().execute())
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("at least one item");
    }

    @Test
    void transact_transactionCancelled_conditionFailure_shouldThrowConditionFailed() {
        CancellationReason reason = CancellationReason.builder()
                .code("ConditionalCheckFailed")
                .message("condition not met")
                .build();
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenThrow(TransactionCanceledException.builder()
                        .message("cancelled")
                        .cancellationReasons(reason)
                        .build());

        assertThatThrownBy(() ->
                transactionOperation.transact()
                        .put(new SimpleItem("id-1", "test", 10))
                        .execute()
        ).isInstanceOf(DynamoConditionFailedException.class)
                .hasMessageContaining("condition check failure");
    }

    @Test
    void transact_transactionCancelled_otherReason_shouldThrowTransactionException() {
        CancellationReason reason = CancellationReason.builder()
                .code("TransactionConflict")
                .message("conflict")
                .build();
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenThrow(TransactionCanceledException.builder()
                        .message("cancelled")
                        .cancellationReasons(reason)
                        .build());

        assertThatThrownBy(() ->
                transactionOperation.transact()
                        .put(new SimpleItem("id-1", "test", 10))
                        .execute()
        ).isInstanceOf(DynamoTransactionException.class)
                .satisfies(ex -> {
                    DynamoTransactionException txEx = (DynamoTransactionException) ex;
                    assertThat(txEx.getCancellationReasons()).isNotEmpty();
                    assertThat(txEx.getCancellationReasons().get(0)).contains("TransactionConflict");
                });
    }

    @Test
    void transact_dynamoDbException_shouldWrapInTransactionException() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenThrow(DynamoDbException.builder().message("error").build());

        assertThatThrownBy(() ->
                transactionOperation.transact()
                        .put(new SimpleItem("id-1", "test", 10))
                        .execute()
        ).isInstanceOf(DynamoTransactionException.class)
                .hasMessageContaining("Transaction failed");
    }

    @Test
    void transact_updateWithCompositeKey_shouldIncludeSortKey() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        transactionOperation.transact()
                .update(GameItem.class, "game-1", "Zelda", u -> u
                        .set("rating", 9.9))
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        Update update = captor.getValue().transactItems().get(0).update();
        assertThat(update.key()).containsKey("game_id");
        assertThat(update.key()).containsKey("title");
    }

    @Test
    void transact_updateWithDecrement_shouldGenerateDecrementExpression() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        transactionOperation.transact()
                .update(SimpleItem.class, "id-1", u -> u.decrement("count", 5))
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        Update update = captor.getValue().transactItems().get(0).update();
        assertThat(update.updateExpression()).contains(" - ");
    }

    @Test
    void transact_updateWithRemove_shouldGenerateRemoveExpression() {
        when(dynamoDbClient.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        transactionOperation.transact()
                .update(SimpleItem.class, "id-1", u -> u.remove("tempField"))
                .execute();

        ArgumentCaptor<TransactWriteItemsRequest> captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(dynamoDbClient).transactWriteItems(captor.capture());

        Update update = captor.getValue().transactItems().get(0).update();
        assertThat(update.updateExpression()).contains("REMOVE");
    }

    @Test
    void transact_updateWithNoClause_shouldThrow() {
        assertThatThrownBy(() ->
                transactionOperation.transact()
                        .update(SimpleItem.class, "id-1", u -> {})
                        .execute()
        ).isInstanceOf(DynamoException.class)
                .hasMessageContaining("at least one SET, REMOVE, ADD, or DELETE");
    }

    // ======== TransactGetItems Tests ========

    @Test
    void transactGet_shouldSendTransactGetItemsRequest() {
        ItemResponse itemResp = ItemResponse.builder()
                .item(Map.of(
                        "item_id", AttributeValue.builder().s("id-1").build(),
                        "name", AttributeValue.builder().s("test").build(),
                        "count", AttributeValue.builder().n("10").build()
                )).build();

        when(dynamoDbClient.transactGetItems(any(TransactGetItemsRequest.class)))
                .thenReturn(TransactGetItemsResponse.builder()
                        .responses(itemResp)
                        .build());

        TransactionOperation.TransactGetResult result = transactionOperation.transactGet()
                .get(SimpleItem.class, "id-1")
                .execute();

        assertThat(result.size()).isEqualTo(1);
        SimpleItem item = result.get(0, SimpleItem.class);
        assertThat(item).isNotNull();
        assertThat(item.getId()).isEqualTo("id-1");
        assertThat(item.getName()).isEqualTo("test");
        assertThat(item.getCount()).isEqualTo(10);
    }

    @Test
    void transactGet_multipleItems_shouldReturnInOrder() {
        ItemResponse resp1 = ItemResponse.builder()
                .item(Map.of("item_id", AttributeValue.builder().s("id-1").build()))
                .build();
        ItemResponse resp2 = ItemResponse.builder()
                .item(Map.of(
                        "game_id", AttributeValue.builder().s("game-1").build(),
                        "title", AttributeValue.builder().s("Zelda").build()
                )).build();

        when(dynamoDbClient.transactGetItems(any(TransactGetItemsRequest.class)))
                .thenReturn(TransactGetItemsResponse.builder()
                        .responses(resp1, resp2)
                        .build());

        TransactionOperation.TransactGetResult result = transactionOperation.transactGet()
                .get(SimpleItem.class, "id-1")
                .get(GameItem.class, "game-1", "Zelda")
                .execute();

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0, SimpleItem.class).getId()).isEqualTo("id-1");
        assertThat(result.get(1, GameItem.class).getGameId()).isEqualTo("game-1");
    }

    @Test
    void transactGet_itemNotFound_shouldReturnNull() {
        ItemResponse emptyResp = ItemResponse.builder().item(Map.of()).build();

        when(dynamoDbClient.transactGetItems(any(TransactGetItemsRequest.class)))
                .thenReturn(TransactGetItemsResponse.builder()
                        .responses(emptyResp)
                        .build());

        TransactionOperation.TransactGetResult result = transactionOperation.transactGet()
                .get(SimpleItem.class, "nonexistent")
                .execute();

        assertThat(result.get(0, SimpleItem.class)).isNull();
    }

    @Test
    void transactGet_indexOutOfBounds_shouldThrow() {
        ItemResponse resp = ItemResponse.builder()
                .item(Map.of("item_id", AttributeValue.builder().s("id-1").build()))
                .build();

        when(dynamoDbClient.transactGetItems(any(TransactGetItemsRequest.class)))
                .thenReturn(TransactGetItemsResponse.builder()
                        .responses(resp)
                        .build());

        TransactionOperation.TransactGetResult result = transactionOperation.transactGet()
                .get(SimpleItem.class, "id-1")
                .execute();

        assertThatThrownBy(() -> result.get(5, SimpleItem.class))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("out of bounds");
    }

    @Test
    void transactGet_empty_shouldThrow() {
        assertThatThrownBy(() -> transactionOperation.transactGet().execute())
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("at least one item");
    }

    @Test
    void transactGet_dynamoDbException_shouldWrapInTransactionException() {
        when(dynamoDbClient.transactGetItems(any(TransactGetItemsRequest.class)))
                .thenThrow(DynamoDbException.builder().message("error").build());

        assertThatThrownBy(() ->
                transactionOperation.transactGet()
                        .get(SimpleItem.class, "id-1")
                        .execute()
        ).isInstanceOf(DynamoTransactionException.class)
                .hasMessageContaining("TransactGet failed");
    }
}

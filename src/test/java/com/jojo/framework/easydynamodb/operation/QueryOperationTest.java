package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.testmodel.GameItem;
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
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueryOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private GetOperation getOperation;
    private QueryOperation queryOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        getOperation = new GetOperation(dynamoDbClient, metadataRegistry);
        queryOperation = new QueryOperation(dynamoDbClient, metadataRegistry, getOperation);
    }

    @Test
    void query_shouldReturnResults() {
        Map<String, AttributeValue> item = Map.of(
                "game_id", AttributeValue.builder().s("g1").build(),
                "title", AttributeValue.builder().s("Zelda").build(),
                "rating", AttributeValue.builder().n("9.5").build()
        );
        QueryResponse response = QueryResponse.builder()
                .items(List.of(item))
                .build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        var result = queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .expressionValues(Map.of(":pk", AttributeValue.builder().s("g1").build()))
                .execute();

        assertThat(result.items()).hasSize(1);
        assertThat(result.items().get(0).getGameId()).isEqualTo("g1");
        assertThat(result.hasMorePages()).isFalse();
    }

    @Test
    void query_withoutKeyCondition_shouldThrow() {
        assertThatThrownBy(() -> queryOperation.query(GameItem.class).execute())
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("keyCondition is required");
    }

    @Test
    void query_withIndex_shouldSetIndexName() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .expressionValues(Map.of(":pk", AttributeValue.builder().s("g1").build()))
                .index("my-gsi")
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        assertThat(captor.getValue().indexName()).isEqualTo("my-gsi");
    }

    @Test
    void query_descending_shouldSetScanForwardFalse() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .expressionValues(Map.of(":pk", AttributeValue.builder().s("g1").build()))
                .descending()
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        assertThat(captor.getValue().scanIndexForward()).isFalse();
    }

    @Test
    void query_valueShorthand_shouldAutoConvert() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk AND title = :title")
                .value(":pk", "g1")
                .value(":title", "Zelda")
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        Map<String, AttributeValue> values = captor.getValue().expressionAttributeValues();
        assertThat(values.get(":pk").s()).isEqualTo("g1");
        assertThat(values.get(":title").s()).isEqualTo("Zelda");
    }

    @Test
    void query_valueShorthand_withNumber_shouldAutoConvert() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .value(":pk", "g1")
                .filter("rating > :min")
                .value(":min", 9.0)
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        assertThat(captor.getValue().expressionAttributeValues().get(":min").n()).isEqualTo("9.0");
    }

    @Test
    void query_valueMixedWithExpressionValues_shouldMerge() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .expressionValues(Map.of(":pk", AttributeValue.builder().s("g1").build()))
                .value(":extra", "bonus")
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        Map<String, AttributeValue> values = captor.getValue().expressionAttributeValues();
        assertThat(values).containsKey(":pk");
        assertThat(values).containsKey(":extra");
    }

    @Test
    void query_consistentRead_shouldSetOnRequest() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .value(":pk", "g1")
                .consistentRead(true)
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        assertThat(captor.getValue().consistentRead()).isTrue();
    }

    @Test
    void query_projection_shouldSetOnRequest() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .value(":pk", "g1")
                .projection("game_id, title, rating")
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        assertThat(captor.getValue().projectionExpression()).isEqualTo("game_id, title, rating");
    }

    @Test
    void query_defaultConsistentRead_shouldNotBeSet() {
        QueryResponse response = QueryResponse.builder().items(List.of()).build();
        when(dynamoDbClient.query(any(QueryRequest.class))).thenReturn(response);

        queryOperation.query(GameItem.class)
                .keyCondition("game_id = :pk")
                .value(":pk", "g1")
                .execute();

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(dynamoDbClient).query(captor.capture());
        // When not set, should be null (DynamoDB defaults to false)
        assertThat(captor.getValue().consistentRead()).isNull();
    }

    @Test
    void queryResult_hasMorePages_shouldReflectLastEvaluatedKey() {
        var result = new QueryOperation.QueryResult<>(List.of(), Map.of("pk", AttributeValue.builder().s("x").build()));
        assertThat(result.hasMorePages()).isTrue();

        var noMore = new QueryOperation.QueryResult<>(List.of(), null);
        assertThat(noMore.hasMorePages()).isFalse();
    }
}

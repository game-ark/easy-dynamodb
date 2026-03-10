package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ScanOperationTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private MetadataRegistry metadataRegistry;
    private GetOperation getOperation;
    private ScanOperation scanOperation;

    @BeforeEach
    void setUp() {
        metadataRegistry = new MetadataRegistry(new ConverterRegistry(), "");
        getOperation = new GetOperation(dynamoDbClient, metadataRegistry);
        scanOperation = new ScanOperation(dynamoDbClient, metadataRegistry, getOperation);
    }

    @Test
    void scan_shouldReturnResults() {
        Map<String, AttributeValue> item = Map.of(
                "item_id", AttributeValue.builder().s("id-1").build(),
                "name", AttributeValue.builder().s("test").build()
        );
        ScanResponse response = ScanResponse.builder()
                .items(List.of(item))
                .build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        var result = scanOperation.scan(SimpleItem.class).execute();

        assertThat(result.items()).hasSize(1);
        assertThat(result.items().get(0).getId()).isEqualTo("id-1");
    }

    @Test
    void scan_withFilter_shouldSetFilterExpression() {
        ScanResponse response = ScanResponse.builder().items(List.of()).build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        scanOperation.scan(SimpleItem.class)
                .filter("count > :min")
                .expressionValues(Map.of(":min", AttributeValue.builder().n("5").build()))
                .execute();

        ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(dynamoDbClient).scan(captor.capture());
        assertThat(captor.getValue().filterExpression()).isEqualTo("count > :min");
    }

    @Test
    void scan_withLimit_shouldSetLimit() {
        ScanResponse response = ScanResponse.builder().items(List.of()).build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(response);

        scanOperation.scan(SimpleItem.class).limit(10).execute();

        ArgumentCaptor<ScanRequest> captor = ArgumentCaptor.forClass(ScanRequest.class);
        verify(dynamoDbClient).scan(captor.capture());
        assertThat(captor.getValue().limit()).isEqualTo(10);
    }

    @Test
    void scanAll_shouldPaginateAutomatically() {
        Map<String, AttributeValue> item1 = Map.of(
                "item_id", AttributeValue.builder().s("id-1").build()
        );
        Map<String, AttributeValue> item2 = Map.of(
                "item_id", AttributeValue.builder().s("id-2").build()
        );

        // First page with lastEvaluatedKey
        ScanResponse page1 = ScanResponse.builder()
                .items(List.of(item1))
                .lastEvaluatedKey(Map.of("item_id", AttributeValue.builder().s("id-1").build()))
                .build();
        // Second page without lastEvaluatedKey
        ScanResponse page2 = ScanResponse.builder()
                .items(List.of(item2))
                .build();

        when(dynamoDbClient.scan(any(ScanRequest.class)))
                .thenReturn(page1)
                .thenReturn(page2);

        List<SimpleItem> results = scanOperation.scan(SimpleItem.class).executeAll();

        assertThat(results).hasSize(2);
        verify(dynamoDbClient, times(2)).scan(any(ScanRequest.class));
    }
}

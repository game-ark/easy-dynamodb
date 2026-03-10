package com.jojo.framework.easydynamodb.testmodel;

import com.jojo.framework.easydynamodb.annotation.DynamoTable;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * Test entity with partition key only (no sort key).
 */
@DynamoTable("simple_items")
public class SimpleItem {

    private String id;
    private String name;
    private Integer count;

    public SimpleItem() {}

    public SimpleItem(String id, String name, Integer count) {
        this.id = id;
        this.name = name;
        this.count = count;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("item_id")
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Integer getCount() { return count; }
    public void setCount(Integer count) { this.count = count; }
}

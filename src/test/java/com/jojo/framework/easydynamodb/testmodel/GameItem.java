package com.jojo.framework.easydynamodb.testmodel;

import com.jojo.framework.easydynamodb.annotation.DynamoTable;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

/**
 * Test entity with partition key + sort key.
 */
@DynamoTable("games")
public class GameItem {

    private String gameId;
    private String title;
    private Double rating;
    private Integer year;
    private Boolean active;

    public GameItem() {}

    public GameItem(String gameId, String title, Double rating, Integer year, Boolean active) {
        this.gameId = gameId;
        this.title = title;
        this.rating = rating;
        this.year = year;
        this.active = active;
    }

    @DynamoDbPartitionKey
    @DynamoDbAttribute("game_id")
    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    @DynamoDbSortKey
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    @DynamoDbAttribute("rating")
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }

    public Integer getYear() { return year; }
    public void setYear(Integer year) { this.year = year; }

    public Boolean getActive() { return active; }
    public void setActive(Boolean active) { this.active = active; }
}

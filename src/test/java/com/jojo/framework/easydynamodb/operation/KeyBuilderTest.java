package com.jojo.framework.easydynamodb.operation;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.exception.DynamoException;
import com.jojo.framework.easydynamodb.metadata.EntityMetadata;
import com.jojo.framework.easydynamodb.metadata.MetadataRegistry;
import com.jojo.framework.easydynamodb.testmodel.GameItem;
import com.jojo.framework.easydynamodb.testmodel.SimpleItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyBuilderTest {

    private MetadataRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetadataRegistry(new ConverterRegistry(), "");
        registry.register(GameItem.class);
        registry.register(SimpleItem.class);
    }

    @Test
    void buildKeyMap_withPkAndSk() {
        EntityMetadata metadata = registry.getMetadata(GameItem.class);
        Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, "game-1", "Zelda");

        assertThat(keyMap).containsKey("game_id");
        assertThat(keyMap.get("game_id").s()).isEqualTo("game-1");
        assertThat(keyMap).containsKey("title");
        assertThat(keyMap.get("title").s()).isEqualTo("Zelda");
    }

    @Test
    void buildKeyMap_pkOnly_noSortKeyEntity() {
        EntityMetadata metadata = registry.getMetadata(SimpleItem.class);
        Map<String, AttributeValue> keyMap = KeyBuilder.buildKeyMap(metadata, "item-1", null);

        assertThat(keyMap).containsKey("item_id");
        assertThat(keyMap.get("item_id").s()).isEqualTo("item-1");
        assertThat(keyMap).hasSize(1);
    }

    @Test
    void buildKeyMap_sortKeyProvided_butEntityHasNoSk_shouldThrow() {
        EntityMetadata metadata = registry.getMetadata(SimpleItem.class);
        assertThatThrownBy(() -> KeyBuilder.buildKeyMap(metadata, "item-1", "unexpected-sk"))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("Sort key provided but entity");
    }

    @Test
    void buildKeyMap_sortKeyRequired_butNotProvided_shouldThrow() {
        EntityMetadata metadata = registry.getMetadata(GameItem.class);
        assertThatThrownBy(() -> KeyBuilder.buildKeyMap(metadata, "game-1", null))
                .isInstanceOf(DynamoException.class)
                .hasMessageContaining("no sort key value was provided");
    }

    @Test
    void extractKeyDescription_shouldReturnReadableString() {
        EntityMetadata metadata = registry.getMetadata(GameItem.class);
        Map<String, AttributeValue> item = Map.of(
                "game_id", AttributeValue.builder().s("g1").build(),
                "title", AttributeValue.builder().s("Zelda").build()
        );
        String desc = KeyBuilder.extractKeyDescription(item, metadata);
        assertThat(desc).contains("game_id").contains("g1").contains("title").contains("Zelda");
    }
}

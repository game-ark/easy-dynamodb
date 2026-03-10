package com.jojo.framework.easydynamodb.metadata;

import com.jojo.framework.easydynamodb.converter.ConverterRegistry;
import com.jojo.framework.easydynamodb.exception.DynamoConfigException;
import com.jojo.framework.easydynamodb.testmodel.GameItem;
import com.jojo.framework.easydynamodb.testmodel.SimpleItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataRegistryTest {

    private MetadataRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetadataRegistry(new ConverterRegistry(), "");
    }

    @Test
    void register_shouldParseGameItem() {
        registry.register(GameItem.class);
        EntityMetadata metadata = registry.getMetadata(GameItem.class);

        assertThat(metadata.getTableName()).isEqualTo("games");
        assertThat(metadata.getPartitionKey().getDynamoAttributeName()).isEqualTo("game_id");
        assertThat(metadata.getSortKey()).isNotNull();
        assertThat(metadata.getSortKey().getDynamoAttributeName()).isEqualTo("title");
    }

    @Test
    void register_shouldParseSimpleItem() {
        registry.register(SimpleItem.class);
        EntityMetadata metadata = registry.getMetadata(SimpleItem.class);

        assertThat(metadata.getTableName()).isEqualTo("simple_items");
        assertThat(metadata.getPartitionKey().getDynamoAttributeName()).isEqualTo("item_id");
        assertThat(metadata.getSortKey()).isNull();
    }

    @Test
    void register_withPrefix_shouldPrependPrefix() {
        MetadataRegistry prefixed = new MetadataRegistry(new ConverterRegistry(), "dev_");
        prefixed.register(GameItem.class);
        assertThat(prefixed.getMetadata(GameItem.class).getTableName()).isEqualTo("dev_games");
    }

    @Test
    void register_idempotent_shouldNotThrow() {
        registry.register(GameItem.class);
        registry.register(GameItem.class); // should not throw
        assertThat(registry.isRegistered(GameItem.class)).isTrue();
    }

    @Test
    void getMetadata_unregistered_shouldThrow() {
        assertThatThrownBy(() -> registry.getMetadata(GameItem.class))
                .isInstanceOf(DynamoConfigException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    void isRegistered_shouldReturnCorrectState() {
        assertThat(registry.isRegistered(GameItem.class)).isFalse();
        registry.register(GameItem.class);
        assertThat(registry.isRegistered(GameItem.class)).isTrue();
    }

    @Test
    void fields_shouldIncludeAllNonKeyFields() {
        registry.register(GameItem.class);
        EntityMetadata metadata = registry.getMetadata(GameItem.class);
        // gameId, title, rating, year, active = 5 fields total
        assertThat(metadata.getFields()).hasSizeGreaterThanOrEqualTo(5);
    }

    @Test
    void newInstance_shouldCreateEntity() {
        registry.register(SimpleItem.class);
        EntityMetadata metadata = registry.getMetadata(SimpleItem.class);
        Object instance = metadata.newInstance();
        assertThat(instance).isInstanceOf(SimpleItem.class);
    }

    @Test
    void fieldMetadata_shouldReadAndWriteValues() {
        registry.register(SimpleItem.class);
        EntityMetadata metadata = registry.getMetadata(SimpleItem.class);

        SimpleItem item = new SimpleItem("id-1", "test", 42);
        FieldMetadata pkField = metadata.getPartitionKey();
        assertThat(pkField.getValue(item)).isEqualTo("id-1");

        pkField.setValue(item, "id-2");
        assertThat(item.getId()).isEqualTo("id-2");
    }

    @Test
    void customTableNameResolver_shouldBeUsed() {
        TableNameResolver custom = new TableNameResolver() {
            @Override
            public String resolve(String tableName, String prefix) {
                return "custom_" + tableName;
            }
        };
        MetadataRegistry customRegistry = new MetadataRegistry(new ConverterRegistry(), "", custom);
        customRegistry.register(SimpleItem.class);
        assertThat(customRegistry.getMetadata(SimpleItem.class).getTableName()).isEqualTo("custom_simple_items");
    }
}

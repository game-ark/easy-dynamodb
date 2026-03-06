English | [中文](README_CN.md)

# EasyDynamodb

Lightweight Java DynamoDB library. Annotation-driven, zero config files, one `DDM` class for all CRUD operations.

```java
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")
    .autoCreateTable(true)
    .build();

ddm.save(user);                                    // Save
User user = ddm.get(User.class, "user-001");       // Get
ddm.update(user, u -> u.setName("New Name"));      // Partial update
ddm.saveBatch(userList);                           // Batch save
```

## Requirements

- Java 21+
- AWS SDK v2

## Maven Dependency

```xml
<dependency>
    <groupId>games.jojocat.framework</groupId>
    <artifactId>easy-dynamodb</artifactId>
    <version>1.0.0</version>
</dependency>
```

---

## Full Example

A game data table demonstrating all core features.

### 1. Define Entity

```java
import com.jojo.framework.easydynamodb.annotation.DynamoTable;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;
import java.util.List;

@DynamoTable("game")
public class Game {

    private String gameId;
    private String version;
    private String title;
    private String genre;
    private Double rating;
    private Integer playerCount;
    private List<String> tags;
    private String internalNote;

    // ---- Primary Key ----

    @DynamoDbPartitionKey                          // Partition key (required)
    @DynamoDbAttribute("game_id")                  // Custom DynamoDB attribute name
    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    @DynamoDbSortKey                               // Sort key (optional)
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    // ---- GSI (Global Secondary Index) ----

    @DynamoDbSecondaryPartitionKey(indexNames = "genre-rating-index")
    public String getGenre() { return genre; }
    public void setGenre(String genre) { this.genre = genre; }

    @DynamoDbSecondarySortKey(indexNames = "genre-rating-index")
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }

    // ---- Regular Fields ----

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public Integer getPlayerCount() { return playerCount; }
    public void setPlayerCount(Integer playerCount) { this.playerCount = playerCount; }

    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }

    // ---- Ignored Field ----

    @DynamoDbIgnore                                // Not written to DynamoDB
    public String getInternalNote() { return internalNote; }
    public void setInternalNote(String internalNote) { this.internalNote = internalNote; }
}
```

### 2. Initialize DDM

```java
import com.jojo.framework.easydynamodb.DDM;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

DynamoDbClient client = DynamoDbClient.create();

// Option 1: Quick create with defaults
DDM ddm = DDM.create(client);

// Option 2: Builder with custom configuration
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")              // Prefix all table names → "prod_game"
    .autoCreateTable(true)             // Auto-create table on first save (default: off)
    .register(Game.class)              // Pre-register entity (optional, auto-registered on first use)
    .build();
```

### 3. Save

```java
Game game = new Game();
game.setGameId("zelda-001");
game.setVersion("v1.0");
game.setTitle("The Legend of Zelda");
game.setGenre("RPG");
game.setRating(9.8);
game.setPlayerCount(1);
game.setTags(List.of("adventure", "openworld"));

// Save single item (null fields are skipped, not written to DynamoDB)
ddm.save(game);

// Batch save (auto-splits into batches of 25, parallel execution)
ddm.saveBatch(gameList);
```

### 4. Get

```java
// Partition key only
Game game = ddm.get(Game.class, "zelda-001");

// Partition key + sort key
Game game = ddm.get(Game.class, "zelda-001", "v1.0");

// Returns null if not found
Game notFound = ddm.get(Game.class, "xxx", "v0");  // → null

// Batch get (auto-splits into batches of 100, parallel execution)
List<Game> games = ddm.getBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));
```

### 5. Partial Update

```java
Game game = new Game();
game.setGameId("zelda-001");
game.setVersion("v1.0");

// Only update specified fields, other fields remain unchanged
ddm.update(game, g -> {
    g.setTitle("The Legend of Zelda: Tears of the Kingdom");
    g.setRating(9.9);
});

// Full update (all non-null, non-key fields)
game.setTitle("The Legend of Zelda: Tears of the Kingdom");
game.setRating(9.9);
game.setPlayerCount(1);
ddm.updateAll(game);
```

> How `update` works internally: creates an empty object with only the primary key set → passes it to your lambda → compares which fields were modified → sends only those fields as an UpdateExpression. An empty lambda sends no request.

---

## Annotations

EasyDynamodb reuses AWS Enhanced Client annotations directly. Only `@DynamoTable` and `@DynamoConverter` are library-specific.

| Annotation | Target | Description |
|-----------|--------|-------------|
| `@DynamoTable("name")` | Class | Specifies DynamoDB table name. Defaults to class name if omitted. **Library-specific** |
| `@DynamoDbBean` | Class | AWS annotation, also usable as entity marker (table name = class name) |
| `@DynamoDbPartitionKey` | Getter | Partition key (exactly one required) |
| `@DynamoDbSortKey` | Getter | Sort key (optional) |
| `@DynamoDbAttribute("name")` | Getter | Custom DynamoDB attribute name |
| `@DynamoDbIgnore` | Getter | Ignore field, not read/written to DynamoDB |
| `@DynamoDbSecondaryPartitionKey(indexNames={"idx"})` | Getter | GSI partition key |
| `@DynamoDbSecondarySortKey(indexNames={"idx"})` | Getter | GSI sort key |
| `@DynamoConverter(XxxConverter.class)` | Field | Specify custom type converter. **Library-specific** |

---

## Type Mapping

14 built-in Java type converters, no manual handling required.

| Java Type | DynamoDB Type | Notes |
|-----------|--------------|-------|
| `String` | S | |
| `Integer` `Long` `Float` `Double` `BigDecimal` | N | |
| `Boolean` | BOOL | |
| `byte[]` | B | Binary |
| `List<T>` | L | Recursively converts elements |
| `Set<String>` | SS | String set |
| `Set<Integer>` `Set<Long>` etc. | NS | Number set |
| `Map<String, T>` | M | Recursively converts values |
| Nested entity | M | Recursively converts using entity metadata |
| `Instant` | S | ISO-8601 |
| `LocalDateTime` | S | ISO-8601 |

---

## Custom Converter

When built-in types don't meet your needs, implement the `AttributeConverter<T>` interface:

```java
import com.jojo.framework.easydynamodb.converter.AttributeConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class StatusConverter implements AttributeConverter<Status> {

    @Override
    public AttributeValue toAttributeValue(Status value) {
        return AttributeValue.builder().s(value.name()).build();
    }

    @Override
    public Status fromAttributeValue(AttributeValue av) {
        return Status.valueOf(av.s());
    }

    @Override
    public Class<Status> targetType() {
        return Status.class;
    }
}
```

Two registration methods:

```java
// Method 1: Field-level — annotation
@DynamoConverter(StatusConverter.class)
private Status status;

// Method 2: Global — Builder registration (applies to all fields of this type)
DDM ddm = DDM.builder(client)
    .registerConverter(Status.class, new StatusConverter())
    .build();
```

---

## Auto-Create Table

Disabled by default. Suitable for dev/test environments. Automatically creates the table based on entity annotations on first `save` if the table doesn't exist.

```java
DDM ddm = DDM.builder(client)
    .autoCreateTable(true)
    .build();

ddm.save(game);  // Table doesn't exist → auto-create → save
```

Auto-create behavior:
- Billing mode: `PAY_PER_REQUEST` (on-demand)
- Infers primary key from `@DynamoDbPartitionKey` / `@DynamoDbSortKey`
- Infers GSIs from `@DynamoDbSecondaryPartitionKey` / `@DynamoDbSecondarySortKey` (Projection = ALL)
- Waits for table status ACTIVE before saving

> ⚠️ Recommended to disable in production. Use IaC tools to manage table schemas.

---

## Table Name Prefix & Custom Resolver

### Unified Prefix

```java
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")   // @DynamoTable("game") → actual table name "prod_game"
    .build();
```

### Custom Table Name Resolver

Extend `TableNameResolver` and override the `resolve` method:

```java
import com.jojo.framework.easydynamodb.metadata.TableNameResolver;

public class TenantResolver extends TableNameResolver {
    private final String tenantId;

    public TenantResolver(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String resolve(String tableName, String prefix) {
        // tableName = raw name from annotation, prefix = configured in Builder
        return prefix + tenantId + "_" + tableName;
    }
}
```

```java
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")
    .tableNameResolver(new TenantResolver("company_a"))
    .build();
// @DynamoTable("game") → "prod_company_a_game"
```

---

## Batch Operations

Auto-splitting, parallel execution, exponential backoff retry.

```java
// Batch save (25 items per batch)
ddm.saveBatch(gameList);

// Batch get (100 items per batch)
List<Game> games = ddm.getBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));
```

- Automatically splits when exceeding single batch limit
- Multiple batches sent in parallel
- Unprocessed items automatically retried (exponential backoff, max 3 retries)
- Throws `DynamoBatchException` if items still fail after retries

---

## Exception Hierarchy

All exceptions extend `RuntimeException`, no try-catch required.

```
RuntimeException
└── DynamoException                    // Base class, wraps AWS SDK exceptions
    ├── DynamoConfigException          // Annotation config errors (missing PK, no converter, etc.)
    ├── DynamoConversionException      // Type conversion failure (includes field name, source/target type)
    └── DynamoBatchException           // Batch operation partial failure (includes failed items list)
```

---

## Performance Design

- Metadata parsed once, cached in `ConcurrentHashMap`, zero reflection at runtime
- Field access via `MethodHandle`, near-direct invocation speed after JIT
- Converters bound to fields at registration time, zero lookup overhead at runtime
- Batch operations execute in parallel, fully leveraging DynamoDB's distributed nature

---

## Project Structure

```
com.jojo.framework.easydynamodb
├── DDM.java                           // Core entry point (save/get/update/saveBatch/getBatch)
├── annotation/
│   ├── DynamoTable.java               // Table name annotation
│   └── DynamoConverter.java           // Custom converter annotation
├── metadata/
│   ├── MetadataRegistry.java          // Metadata registry (parses annotations, caches)
│   ├── EntityMetadata.java            // Entity metadata (table name, keys, fields, GSIs)
│   ├── FieldMetadata.java             // Field metadata (attribute name, MethodHandle, converter)
│   ├── GsiMetadata.java               // GSI metadata (index name, partition key, sort key)
│   └── TableNameResolver.java         // Table name resolver (extendable)
├── converter/
│   ├── AttributeConverter.java        // Converter interface
│   ├── ConverterRegistry.java         // Converter registry (14 built-in types)
│   └── builtin/                       // Built-in converters
│       ├── StringConverter
│       ├── NumberConverter            // Integer/Long/Double/Float/BigDecimal
│       ├── BooleanConverter
│       ├── BinaryConverter            // byte[]
│       ├── ListConverter              // List<T> → L
│       ├── SetConverter               // Set<String> → SS, Set<Number> → NS
│       ├── MapConverter               // Map<String,T> → M
│       ├── NestedEntityConverter      // Nested entity → M
│       ├── InstantConverter           // Instant → S (ISO-8601)
│       └── LocalDateTimeConverter     // LocalDateTime → S (ISO-8601)
├── operation/
│   ├── SaveOperation.java            // PutItem + auto-create table
│   ├── GetOperation.java             // GetItem
│   ├── UpdateOperation.java          // UpdateItem (mutator pattern + full update)
│   ├── BatchOperation.java           // BatchWriteItem / BatchGetItem
│   └── TableCreateOperation.java     // CreateTable (with GSI)
├── model/
│   └── KeyPair.java                  // Key pair for batch get
└── exception/
    ├── DynamoException.java
    ├── DynamoConfigException.java
    ├── DynamoConversionException.java
    └── DynamoBatchException.java
```

## License

MIT

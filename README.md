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
ddm.delete(User.class, "user-001");                // Delete
List<User> users = ddm.query(User.class)           // Query
    .keyCondition("pk = :pk")
    .expressionValues(Map.of(":pk", AttributeValue.builder().s("user-001").build()))
    .executeAll();
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

## Core API Overview

| Category | Method | Description |
|----------|--------|-------------|
| Save | `save(entity)` | Save single entity |
| Save | `saveBatch(list)` | Batch save (auto-split 25/batch, parallel) |
| Get | `get(Class, pk)` / `get(Class, pk, sk)` | Get single entity by exact key |
| Get | `getBatch(Class, keys)` | Batch get by exact keys (100/batch, parallel) |
| Query | `query(Class)` | Fluent query builder (key conditions, GSI, filter, pagination) |
| Query | `scan(Class)` | Fluent scan builder (filter, pagination) |
| Update | `update(entity, mutator)` | Partial update (only changed fields) |
| Update | `updateAll(entity)` | Full update (all non-key fields) |
| Update | `updateBatch(list, mutator)` | Batch partial update (parallel) |
| Update | `updateAllBatch(list)` | Batch full update (parallel) |
| Delete | `delete(Class, pk)` / `delete(Class, pk, sk)` | Delete by exact key |
| Delete | `deleteBatch(Class, keys)` | Batch delete by keys (25/batch, parallel) |
| Delete | `deleteByCondition(Class, filter, values, names)` | Delete by condition, returns count |

---

## Full Example

### 1. Define Entity

```java
@DynamoTable("game")
public class Game {
    private String gameId;
    private String version;
    private String title;
    private String genre;
    private Double rating;
    private GameStatus status;       // Enum — auto-converted
    private GameDetail detail;       // Nested entity — auto-converted

    @DynamoDbPartitionKey
    @DynamoDbAttribute("game_id")
    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    @DynamoDbSortKey
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    @DynamoDbSecondaryPartitionKey(indexNames = "genre-rating-index")
    public String getGenre() { return genre; }
    public void setGenre(String genre) { this.genre = genre; }

    @DynamoDbSecondarySortKey(indexNames = "genre-rating-index")
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }

    // ... other getters/setters

    @DynamoDbIgnore
    public String getInternalNote() { return internalNote; }
}
```

Key points:
- `@DynamoTable("game")` — maps to DynamoDB table named `game`. If omitted or empty, defaults to the class name.
- `@DynamoDbPartitionKey` / `@DynamoDbSortKey` — placed on getters. Partition key is required, sort key is optional.
- `@DynamoDbAttribute("game_id")` — custom DynamoDB attribute name. If omitted, uses the Java field name.
- `@DynamoDbIgnore` — placed on getter, field will be excluded from DynamoDB mapping.
- `@DynamoDbSecondaryPartitionKey` / `@DynamoDbSecondarySortKey` — GSI key definitions, placed on getters.

Nested entities (`GameDetail` above) are auto-detected if annotated with `@DynamoTable` or `@DynamoDbBean`, and stored as DynamoDB Map (M) type.

### 2. Initialize DDM

```java
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")           // Table name prefix (e.g. "prod_game")
    .autoCreateTable(true)          // Auto-create table on first save if not exists
    .register(Game.class)           // Pre-register entity (optional, auto-registered on first use)
    .enableLogging(true)            // Enable internal logging (default: false)
    .logLevel(Level.DEBUG)          // Set log level: TRACE/DEBUG/INFO/WARN/ERROR (default: INFO)
    .build();
```

#### Builder Options

| Option | Default | Description |
|--------|---------|-------------|
| `tablePrefix(String)` | `""` | Prefix prepended to all table names |
| `autoCreateTable(boolean)` | `false` | Auto-create table (PAY_PER_REQUEST) on first save if table doesn't exist |
| `register(Class<?>...)` | — | Pre-register entity classes at build time |
| `registerConverter(Class, converter)` | — | Register a global custom converter for a type |
| `tableNameResolver(TableNameResolver)` | `prefix + tableName` | Custom table name resolution strategy |
| `batchExecutor(Executor)` | Virtual thread per task | Custom executor for batch parallel operations |
| `enableLogging(boolean)` | `false` | Enable/disable internal logging (SLF4J) |
| `logLevel(Level)` | `INFO` | Minimum log level (only effective when logging is enabled) |

#### Custom Table Name Resolver

Override `TableNameResolver` for advanced table naming strategies (e.g. multi-tenant):

```java
public class TenantTableNameResolver extends TableNameResolver {
    private final String tenantId;

    public TenantTableNameResolver(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String resolve(String tableName, String prefix) {
        return prefix + tenantId + "_" + tableName;
    }
}

DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")
    .tableNameResolver(new TenantTableNameResolver("tenant-42"))
    .build();
// Table name: "prod_tenant-42_game"
```

### 3. Save

```java
ddm.save(game);              // Single save
ddm.saveBatch(gameList);     // Batch save (25/batch, parallel)
```

When `autoCreateTable(true)` is set, the first `save()` call will auto-create the table (with PAY_PER_REQUEST billing and all GSIs) if it doesn't exist, then retry the save.

### 4. Get & Query

```java
// Exact key lookup (DynamoDB GetItem — O(1), cheapest)
Game game = ddm.get(Game.class, "zelda-001", "v1.0");

// Batch exact key lookup (DynamoDB BatchGetItem — 100/batch)
List<Game> games = ddm.getBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));
```

`KeyPair` accepts `(partitionKey, sortKey)` or just `(partitionKey)` for tables without a sort key.

```java
// Conditional query (DynamoDB Query — range conditions, GSI, pagination)
List<Game> rpgGames = ddm.query(Game.class)
    .index("genre-rating-index")
    .keyCondition("genre = :genre AND rating > :min")
    .expressionValues(Map.of(
        ":genre", AttributeValue.builder().s("RPG").build(),
        ":min", AttributeValue.builder().n("9.0").build()
    ))
    .descending()
    .limit(10)
    .executeAll();

// Scan with filter (full table scan)
List<Game> all = ddm.scan(Game.class)
    .filter("rating > :min")
    .expressionValues(Map.of(":min", AttributeValue.builder().n("9.0").build()))
    .executeAll();
```

> `get` vs `query`: `get` is an O(1) exact key lookup (cheapest). `query` supports range conditions, GSI, sorting, and pagination — use it when you need to find multiple items by condition.

#### Pagination

Both `query()` and `scan()` support two execution modes:

- `executeAll()` — auto-paginates and returns all matching items in a single `List<T>`.
- `execute()` — returns a single page as `QueryResult<T>`, which contains `items()` and `lastEvaluatedKey()` for manual pagination.

```java
// Manual pagination
QueryOperation.QueryResult<Game> page = ddm.query(Game.class)
    .keyCondition("genre = :genre")
    .expressionValues(Map.of(":genre", AttributeValue.builder().s("RPG").build()))
    .limit(20)
    .execute();

List<Game> items = page.items();
boolean hasMore = page.hasMorePages();

// Fetch next page
if (hasMore) {
    QueryOperation.QueryResult<Game> nextPage = ddm.query(Game.class)
        .keyCondition("genre = :genre")
        .expressionValues(Map.of(":genre", AttributeValue.builder().s("RPG").build()))
        .limit(20)
        .startKey(page.lastEvaluatedKey())
        .execute();
}
```

### 5. Update

```java
// Partial update — only changed fields are sent
ddm.update(game, g -> {
    g.setTitle("Zelda: Tears of the Kingdom");
    g.setRating(9.9);
});

// Full update — all non-null fields SET, null fields REMOVE
ddm.updateAll(game);

// Batch partial update — parallel execution
ddm.updateBatch(gameList, g -> g.setStatus(GameStatus.ARCHIVED));

// Batch full update — parallel execution
ddm.updateAllBatch(gameList);
```

How partial update works: `update(entity, mutator)` creates a clean entity copy with only key values, applies the mutator, then diffs to detect which fields changed. Only changed fields are sent as `SET` (non-null) or `REMOVE` (null) expressions. If no fields changed, the update is skipped.

### 6. Delete

```java
// Delete by exact key
ddm.delete(Game.class, "zelda-001", "v1.0");

// Batch delete by keys
ddm.deleteBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));

// Delete by condition — returns number of items deleted
int deleted = ddm.deleteByCondition(Game.class,
    "rating < :minRating",
    Map.of(":minRating", AttributeValue.builder().n("5.0").build()),
    null);
System.out.println("Deleted " + deleted + " items");
```

`deleteByCondition` internally performs a scan → extract keys → batch delete loop. The `expressionNames` parameter (last argument) can be `null` if not needed.

---

## Type Mapping

| Java Type | DynamoDB Type | Notes |
|-----------|--------------|-------|
| `String` | S | |
| `Integer` `Long` `Float` `Double` `BigDecimal` | N | Boxed and primitive types both supported |
| `Boolean` | BOOL | Boxed and primitive both supported |
| `byte[]` | B | Binary |
| `Enum` | S | Auto-converted via `name()` |
| `List<T>` | L | Recursive conversion of elements |
| `Set<String>` | SS | String set |
| `Set<Integer>` `Set<Long>` `Set<Double>` ... | NS | Number set (auto-detected by element type) |
| `Map<String, T>` | M | Recursive conversion of values |
| Nested entity (`@DynamoTable`/`@DynamoDbBean`) | M | Auto-detected and recursively converted |
| `Instant` | S | ISO-8601 format |
| `LocalDateTime` | S | ISO-8601 format |

---

## Annotations

| Annotation | Target | Description |
|-----------|--------|-------------|
| `@DynamoTable("name")` | Class | Table name (defaults to class name if empty). Library-specific annotation |
| `@DynamoDbPartitionKey` | Getter | Partition key (required, exactly one per entity) |
| `@DynamoDbSortKey` | Getter | Sort key (optional) |
| `@DynamoDbAttribute("name")` | Getter | Custom DynamoDB attribute name (defaults to Java field name) |
| `@DynamoDbIgnore` | Getter | Exclude field from DynamoDB mapping |
| `@DynamoDbSecondaryPartitionKey(indexNames={"idx"})` | Getter | GSI partition key |
| `@DynamoDbSecondarySortKey(indexNames={"idx"})` | Getter | GSI sort key |
| `@DynamoConverter(XxxConverter.class)` | Field | Custom converter for this field. Library-specific annotation |

Note: `@DynamoTable` and `@DynamoConverter` are library-specific annotations. All `@DynamoDb*` annotations are from the AWS SDK Enhanced Client.

---

## Custom Converter

Implement `AttributeConverter<T>` to handle custom types:

```java
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
    public Class<Status> targetType() { return Status.class; }
}
```

Two ways to register:

```java
// Option 1: Field-level — applies to a single field
@DynamoConverter(StatusConverter.class)
private Status status;

// Option 2: Global — applies to all fields of this type
DDM ddm = DDM.builder(client)
    .registerConverter(Status.class, new StatusConverter())
    .build();
```

Field-level `@DynamoConverter` takes precedence over global registration.

---

## Exception Handling

```
RuntimeException
└── DynamoException                    // Base class for all EasyDynamodb errors
    ├── DynamoConfigException          // Annotation/entity config errors (raised at registration time)
    ├── DynamoConversionException      // Type conversion failures (includes field name, source/target types)
    └── DynamoBatchException           // Batch partial failures (contains list of individual failures)
```

`DynamoBatchException` provides access to individual failures:

```java
try {
    ddm.saveBatch(entities);
} catch (DynamoBatchException e) {
    for (DynamoBatchException.BatchFailure failure : e.getFailures()) {
        System.err.println("Failed key: " + failure.key() + ", reason: " + failure.errorMessage());
    }
}
```

Batch operations (save/get/delete) automatically retry unprocessed items up to 3 times with exponential backoff (100ms, 200ms, 400ms). If items remain unprocessed after retries, a `DynamoBatchException` is thrown.

---

## Logging

EasyDynamodb uses SLF4J for internal logging, disabled by default. Enable it via the builder:

```java
DDM ddm = DDM.builder(client)
    .enableLogging(true)
    .logLevel(Level.DEBUG)    // TRACE / DEBUG / INFO / WARN / ERROR
    .build();
```

| Level | What's logged |
|-------|---------------|
| `ERROR` | Operation failures, retries exhausted |
| `WARN` | Batch retry attempts, partial failures |
| `INFO` | Entity registration, table creation, operation completion |
| `DEBUG` | Operation parameters, changed fields, query conditions |
| `TRACE` | Raw DynamoDB responses, individual chunk results |

You need to provide an SLF4J implementation (e.g. `slf4j-simple`, `logback-classic`, `log4j-slf4j2-impl`) in your runtime classpath.

---

## Performance

- Metadata parsed once per entity class, cached in `ConcurrentHashMap` — zero reflection at runtime
- Field access via `MethodHandle` — near-direct invocation performance after JIT warm-up
- Converters bound at registration time — zero lookup overhead at runtime
- Batch and update operations parallelized via virtual threads (Java 21+)
- Custom `batchExecutor` supported for environments where virtual threads are not desired

## License

MIT

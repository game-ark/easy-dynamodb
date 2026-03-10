English | [中文](README_CN.md)

# EasyDynamodb

Lightweight Java DynamoDB library. Annotation-driven, zero config files, one `DDM` class for all CRUD operations.

> DDM = **D**ynamo**D**ata**M**anager

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
    .value(":pk", "user-001")                      // Auto-converts to AttributeValue
    .executeAll();
```

## Why EasyDynamodb?

AWS SDK Enhanced Client is powerful but verbose. EasyDynamodb trades some flexibility for simplicity:

| | AWS Enhanced Client | EasyDynamodb |
|---|---|---|
| Config files | TableSchema / StaticTableSchema | Zero — annotation-driven |
| CRUD code | Separate Table, Index, Key objects | One `DDM` class, one-liner calls |
| Partial update | Manual UpdateExpression building | `update(entity, mutator)` with auto-diff |
| Batch operations | Manual chunking + retry | Auto-split, parallel, exponential backoff |
| Type conversion | BeanTableSchema or manual | Auto-detected, extensible converters |
| Learning curve | Moderate | Minimal — if you know JPA annotations, you're set |

EasyDynamodb is ideal for projects that want DynamoDB without the boilerplate. If you need fine-grained control over every request parameter, the AWS SDK Enhanced Client is the better choice.

## Requirements

- Java 21+
- AWS SDK v2

## Quick Start

**1. Add Maven dependency:**

```xml
<dependency>
    <groupId>games.jojocat.framework</groupId>
    <artifactId>easy-dynamodb</artifactId>
    <version>1.0.2</version>
</dependency>
```

**2. Define your entity:**

```java
@DynamoTable("users")
public class User {
    private String userId;
    private String name;

    public User() {}  // Required: no-arg constructor

    @DynamoDbPartitionKey
    @DynamoDbAttribute("user_id")
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
```

**3. Start using:**

```java
DynamoDbClient client = DynamoDbClient.create();
DDM ddm = DDM.create(client);

User user = new User();
user.setUserId("u-001");
user.setName("Alice");
ddm.save(user);
User found = ddm.get(User.class, "u-001");
```

That's it. No XML, no YAML, no config files.

---

## Core API Overview

| Category | Method | Description |
|----------|--------|-------------|
| Save | `save(entity)` | Save single entity |
| Save | `saveBatch(list)` | Batch save (auto-split 25/batch, parallel) |
| Get | `get(Class, pk)` / `get(Class, pk, sk)` | Get single entity by exact key |
| Get | `get(Class, pk, consistentRead)` | Get with strongly consistent read |
| Get | `getBatch(Class, keys)` | Batch get by exact keys (100/batch, parallel) |
| Query | `query(Class)` | Fluent query builder (key conditions, GSI, filter, pagination) |
| Query | `scan(Class)` | Fluent scan builder (filter, pagination) |
| Update | `update(entity, mutator)` | Partial update (only changed fields) |
| Update | `updateAll(entity)` | Full update (all non-key fields) |
| Update | `updateBatch(list, mutator)` | Batch partial update (parallel) |
| Update | `updateAllBatch(list)` | Batch full update (parallel) |
| Delete | `delete(Class, pk)` / `delete(Class, pk, sk)` | Delete by exact key |
| Delete | `deleteBatch(Class, keys)` | Batch delete by keys (25/batch, parallel) |
| Delete | `deleteByCondition(Class, filter, values)` | Delete by condition, returns count |
| Delete | `deleteByConditionWithValues(Class, filter, values)` | Delete by condition with auto-converted values |

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

// Strongly consistent read
Game game = ddm.get(Game.class, "zelda-001", "v1.0", true);

// Batch exact key lookup (DynamoDB BatchGetItem — 100/batch)
List<Game> games = ddm.getBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));
```

`KeyPair` accepts `(partitionKey, sortKey)` or just `(partitionKey)` for tables without a sort key.

```java
// Conditional query with value() shorthand
List<Game> rpgGames = ddm.query(Game.class)
    .index("genre-rating-index")
    .keyCondition("genre = :genre AND rating > :min")
    .value(":genre", "RPG")
    .value(":min", 9.0)
    .descending()
    .limit(10)
    .consistentRead(true)
    .executeAll();

// Query with projection (only return specific attributes)
List<Game> titles = ddm.query(Game.class)
    .keyCondition("game_id = :pk")
    .value(":pk", "zelda-001")
    .projection("game_id, version, title")
    .executeAll();

// Scan with filter
List<Game> all = ddm.scan(Game.class)
    .filter("rating > :min")
    .value(":min", 9.0)
    .executeAll();
```

The `value()` method auto-converts Java types to `AttributeValue` — no more verbose `AttributeValue.builder().s("...").build()`. Supported types: String, Number, Boolean, Enum, Instant, LocalDateTime, byte[].

You can still use `expressionValues(Map<String, AttributeValue>)` for full control. Both methods can be mixed — `value()` entries are merged into the expression values map.

> `get` vs `query`: `get` is an O(1) exact key lookup (cheapest). `query` supports range conditions, GSI, sorting, and pagination — use it when you need to find multiple items by condition.

#### Pagination

Both `query()` and `scan()` support two execution modes:

- `executeAll()` — auto-paginates and returns all matching items in a single `List<T>`.
- `execute()` — returns a single page as `PagedResult<T>`, which contains `items()` and `lastEvaluatedKey()` for manual pagination.

```java
// Manual pagination
var page = ddm.query(Game.class)
    .keyCondition("genre = :genre")
    .value(":genre", "RPG")
    .limit(20)
    .execute();

List<Game> items = page.items();
boolean hasMore = page.hasMorePages();

// Fetch next page
if (hasMore) {
    var nextPage = ddm.query(Game.class)
        .keyCondition("genre = :genre")
        .value(":genre", "RPG")
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
    Map.of(":minRating", AttributeValue.builder().n("5.0").build()));
System.out.println("Deleted " + deleted + " items");

// Delete by condition with auto-converted values (recommended)
int deleted2 = ddm.deleteByConditionWithValues(Game.class,
    "rating < :minRating",
    Map.of(":minRating", 5.0));
```

`deleteByCondition` internally performs a scan → extract keys → batch delete loop.

---

## Type Mapping

| Java Type | DynamoDB Type | Notes |
|-----------|--------------|-------|
| `String` | S | |
| `Integer` `Long` `Float` `Double` `Short` `Byte` `BigDecimal` | N | Boxed and primitive types both supported |
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

---

## FAQ

**Q: Does my entity class need a no-arg constructor?**
Yes. EasyDynamodb uses `MethodHandle` to instantiate entities. A public no-arg constructor is required.

**Q: Can I use Lombok `@Data` / `@Builder`?**
`@Data` works if it generates standard getters/setters. `@Builder` alone won't work because it typically removes the no-arg constructor — add `@NoArgsConstructor` alongside it.

**Q: Is `save()` an insert or upsert?**
`save()` maps to DynamoDB `PutItem`, which is an upsert — it overwrites the entire item if the key already exists. There is currently no `saveIfNotExists()` API.

**Q: Can I use this with DynamoDB Local for testing?**
Yes. Point your `DynamoDbClient` to the local endpoint:
```java
DynamoDbClient client = DynamoDbClient.builder()
    .endpointOverride(URI.create("http://localhost:8000"))
    .region(Region.US_EAST_1)
    .build();
DDM ddm = DDM.builder(client).autoCreateTable(true).build();
```

**Q: What happens if a batch operation partially fails?**
Batch operations retry unprocessed items up to 3 times with exponential backoff. If items still remain, a `DynamoBatchException` is thrown containing all individual failures.

---

## Contributing

Contributions are welcome. Please follow these guidelines:

1. Fork the repository and create a feature branch from `main`
2. Follow existing code style (Google Java Style)
3. Add tests for new features — run `mvn test` to verify all tests pass
4. Keep changes focused — one feature or fix per PR
5. Update README if adding user-facing features

### Development Setup

```bash
git clone https://github.com/game-ark/easy-dynamodb.git
mvn clean test    # Requires Java 21+
```

All tests use Mockito — no real DynamoDB connection needed for development.

---

## License

MIT

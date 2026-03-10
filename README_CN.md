[English](README.md) | 中文

# EasyDynamodb

轻量级 Java DynamoDB 操作库。注解驱动，零配置文件，一个 `DDM` 类搞定增删改查。

```java
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")
    .autoCreateTable(true)
    .build();

ddm.save(user);                                    // 保存
User user = ddm.get(User.class, "user-001");       // 获取
ddm.update(user, u -> u.setName("新名字"));          // 部分更新
ddm.delete(User.class, "user-001");                // 删除
List<User> users = ddm.query(User.class)           // 条件查询
    .keyCondition("pk = :pk")
    .expressionValues(Map.of(":pk", AttributeValue.builder().s("user-001").build()))
    .executeAll();
```

## 环境要求

- Java 21+
- AWS SDK v2

## Maven 依赖

```xml
<dependency>
    <groupId>games.jojocat.framework</groupId>
    <artifactId>easy-dynamodb</artifactId>
    <version>1.0.0</version>
</dependency>
```

---

## 核心 API 一览

| 分类 | 方法 | 说明 |
|------|------|------|
| 新增 | `save(entity)` | 保存单条 |
| 新增 | `saveBatch(list)` | 批量保存（25条/批，并行） |
| 查询 | `get(Class, pk)` / `get(Class, pk, sk)` | 按主键精确获取 |
| 查询 | `getBatch(Class, keys)` | 按主键批量获取（100条/批，并行） |
| 查询 | `query(Class)` | 条件查询（范围条件、GSI、过滤、分页） |
| 查询 | `scan(Class)` | 全表扫描（过滤、分页） |
| 更新 | `update(entity, mutator)` | 部分更新（仅变更字段） |
| 更新 | `updateAll(entity)` | 全量更新 |
| 更新 | `updateBatch(list, mutator)` | 批量部分更新（并行） |
| 更新 | `updateAllBatch(list)` | 批量全量更新（并行） |
| 删除 | `delete(Class, pk)` / `delete(Class, pk, sk)` | 按主键删除 |
| 删除 | `deleteBatch(Class, keys)` | 按主键批量删除（25条/批，并行） |
| 删除 | `deleteByCondition(Class, filter, values, names)` | 按条件删除，返回删除条数 |

---

## 完整示例

### 1. 定义实体

```java
@DynamoTable("game")
public class Game {
    private String gameId;
    private String version;
    private String title;
    private String genre;
    private Double rating;
    private GameStatus status;       // 枚举 - 自动转换
    private GameDetail detail;       // 嵌套实体 - 自动转换

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

    // ... 其他 getter/setter

    @DynamoDbIgnore
    public String getInternalNote() { return internalNote; }
}
```

要点说明：
- `@DynamoTable("game")` - 映射到 DynamoDB 表名 `game`。省略或留空时默认使用类名。
- `@DynamoDbPartitionKey` / `@DynamoDbSortKey` - 标注在 getter 上。分区键必须有且仅有一个，排序键可选。
- `@DynamoDbAttribute("game_id")` - 自定义 DynamoDB 属性名。省略时使用 Java 字段名。
- `@DynamoDbIgnore` - 标注在 getter 上，该字段不参与 DynamoDB 映射。
- `@DynamoDbSecondaryPartitionKey` / `@DynamoDbSecondarySortKey` - GSI 键定义，标注在 getter 上。

嵌套实体（如上面的 `GameDetail`）只要标注了 `@DynamoTable` 或 `@DynamoDbBean`，就会被自动检测并以 DynamoDB Map（M）类型存储。

### 2. 初始化 DDM

```java
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")           // 表名前缀（如 "prod_game"）
    .autoCreateTable(true)          // 首次 save 时自动建表
    .register(Game.class)           // 预注册实体（可选，首次使用时也会自动注册）
    .enableLogging(true)            // 开启内部日志（默认关闭）
    .logLevel(Level.DEBUG)          // 日志级别：TRACE/DEBUG/INFO/WARN/ERROR（默认 INFO）
    .build();
```

#### Builder 配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `tablePrefix(String)` | `""` | 表名前缀，拼接在所有表名之前 |
| `autoCreateTable(boolean)` | `false` | 首次 save 时若表不存在则自动创建（PAY_PER_REQUEST 计费模式） |
| `register(Class<?>...)` | - | 在 build 时预注册实体类 |
| `registerConverter(Class, converter)` | - | 注册全局自定义类型转换器 |
| `tableNameResolver(TableNameResolver)` | `prefix + tableName` | 自定义表名解析策略 |
| `batchExecutor(Executor)` | 虚拟线程 | 自定义批量操作的并行执行器 |
| `enableLogging(boolean)` | `false` | 开启/关闭内部日志（基于 SLF4J） |
| `logLevel(Level)` | `INFO` | 最低日志级别（仅在日志开启时生效） |

#### 自定义表名解析器

继承 `TableNameResolver` 可实现高级表名策略（如多租户）：

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
// 最终表名: "prod_tenant-42_game"
```

### 3. 保存

```java
ddm.save(game);              // 单条保存
ddm.saveBatch(gameList);     // 批量保存（25条/批，并行）
```

当设置了 `autoCreateTable(true)` 时，首次 `save()` 如果表不存在，会自动创建表（PAY_PER_REQUEST 计费，包含所有 GSI），然后重试保存。

### 4. 查询

```java
// 精确主键查询（GetItem - O(1)，最便宜）
Game game = ddm.get(Game.class, "zelda-001", "v1.0");

// 批量主键查询（BatchGetItem - 100条/批）
List<Game> games = ddm.getBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));
```

`KeyPair` 接受 `(partitionKey, sortKey)` 或仅 `(partitionKey)`（用于没有排序键的表）。

```java
// 条件查询（Query - 支持范围条件、GSI、排序、分页）
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

// 全表扫描
List<Game> all = ddm.scan(Game.class)
    .filter("rating > :min")
    .expressionValues(Map.of(":min", AttributeValue.builder().n("9.0").build()))
    .executeAll();
```

> `get` vs `query`：`get` 是 O(1) 精确主键查找（最便宜）。`query` 支持范围条件、GSI、排序和分页——需要按条件查找多条记录时使用。

#### 分页

`query()` 和 `scan()` 都支持两种执行模式：

- `executeAll()` - 自动翻页，返回所有匹配项的 `List<T>`。
- `execute()` - 返回单页结果 `QueryResult<T>`，包含 `items()` 和 `lastEvaluatedKey()`，用于手动分页。

```java
// 手动分页
QueryOperation.QueryResult<Game> page = ddm.query(Game.class)
    .keyCondition("genre = :genre")
    .expressionValues(Map.of(":genre", AttributeValue.builder().s("RPG").build()))
    .limit(20)
    .execute();

List<Game> items = page.items();
boolean hasMore = page.hasMorePages();

// 获取下一页
if (hasMore) {
    QueryOperation.QueryResult<Game> nextPage = ddm.query(Game.class)
        .keyCondition("genre = :genre")
        .expressionValues(Map.of(":genre", AttributeValue.builder().s("RPG").build()))
        .limit(20)
        .startKey(page.lastEvaluatedKey())
        .execute();
}
```

### 5. 更新

```java
// 部分更新 - 只发送变更字段
ddm.update(game, g -> {
    g.setTitle("塞尔达：王国之泪");
    g.setRating(9.9);
});

// 全量更新 - 非 null 字段 SET，null 字段 REMOVE
ddm.updateAll(game);

// 批量部分更新（并行）
ddm.updateBatch(gameList, g -> g.setStatus(GameStatus.ARCHIVED));

// 批量全量更新（并行）
ddm.updateAllBatch(gameList);
```

部分更新原理：`update(entity, mutator)` 会创建一个仅包含主键值的干净实体副本，执行 mutator，然后对比差异检测哪些字段发生了变化。只有变更的字段会被发送为 `SET`（非 null）或 `REMOVE`（null）表达式。如果没有字段变化，则跳过更新。

### 6. 删除

```java
// 按主键删除
ddm.delete(Game.class, "zelda-001", "v1.0");

// 批量删除
ddm.deleteBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));

// 按条件删除 - 返回删除条数
int deleted = ddm.deleteByCondition(Game.class,
    "rating < :minRating",
    Map.of(":minRating", AttributeValue.builder().n("5.0").build()),
    null);
System.out.println("已删除 " + deleted + " 条");
```

`deleteByCondition` 内部执行 scan -> 提取主键 -> 批量删除的循环。最后一个参数 `expressionNames` 不需要时可传 `null`。

---

## 类型映射

| Java 类型 | DynamoDB 类型 | 说明 |
|-----------|--------------|------|
| `String` | S | |
| `Integer` `Long` `Float` `Double` `BigDecimal` | N | 包装类型和基本类型均支持 |
| `Boolean` | BOOL | 包装类型和基本类型均支持 |
| `byte[]` | B | 二进制 |
| `Enum` | S | 自动按 `name()` 转换 |
| `List<T>` | L | 递归转换元素 |
| `Set<String>` | SS | 字符串集合 |
| `Set<Integer>` `Set<Long>` `Set<Double>` ... | NS | 数值集合（按元素类型自动检测） |
| `Map<String, T>` | M | 递归转换值 |
| 嵌套实体（`@DynamoTable` / `@DynamoDbBean`） | M | 自动检测并递归转换 |
| `Instant` | S | ISO-8601 格式 |
| `LocalDateTime` | S | ISO-8601 格式 |

---

## 注解一览

| 注解 | 标注位置 | 说明 |
|------|----------|------|
| `@DynamoTable("name")` | 类 | 表名（留空时默认使用类名）。库自有注解 |
| `@DynamoDbPartitionKey` | Getter | 分区键（必须，每个实体有且仅有一个） |
| `@DynamoDbSortKey` | Getter | 排序键（可选） |
| `@DynamoDbAttribute("name")` | Getter | 自定义 DynamoDB 属性名（省略时使用 Java 字段名） |
| `@DynamoDbIgnore` | Getter | 排除字段，不参与 DynamoDB 映射 |
| `@DynamoDbSecondaryPartitionKey(indexNames={"idx"})` | Getter | GSI 分区键 |
| `@DynamoDbSecondarySortKey(indexNames={"idx"})` | Getter | GSI 排序键 |
| `@DynamoConverter(XxxConverter.class)` | 字段 | 指定自定义转换器。库自有注解 |

说明：`@DynamoTable` 和 `@DynamoConverter` 是本库自有注解。所有 `@DynamoDb*` 注解来自 AWS SDK Enhanced Client。

---

## 自定义转换器

实现 `AttributeConverter<T>` 接口来处理自定义类型：

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

两种注册方式：

```java
// 方式一：字段级别 - 仅对该字段生效
@DynamoConverter(StatusConverter.class)
private Status status;

// 方式二：全局注册 - 对所有该类型的字段生效
DDM ddm = DDM.builder(client)
    .registerConverter(Status.class, new StatusConverter())
    .build();
```

字段级别的 `@DynamoConverter` 优先级高于全局注册。

---

## 异常处理

```
RuntimeException
└── DynamoException                    // 所有 EasyDynamodb 错误的基类
    ├── DynamoConfigException          // 注解/实体配置错误（注册阶段抛出）
    ├── DynamoConversionException      // 类型转换失败（包含字段名、源类型、目标类型）
    └── DynamoBatchException           // 批量操作部分失败（包含各项失败详情）
```

`DynamoBatchException` 提供对单项失败的访问：

```java
try {
    ddm.saveBatch(entities);
} catch (DynamoBatchException e) {
    for (DynamoBatchException.BatchFailure failure : e.getFailures()) {
        System.err.println("失败键: " + failure.key() + ", 原因: " + failure.errorMessage());
    }
}
```

批量操作（save/get/delete）会自动重试未处理的项，最多 3 次，采用指数退避（100ms、200ms、400ms）。如果重试后仍有未处理的项，则抛出 `DynamoBatchException`。

---

## 日志

EasyDynamodb 使用 SLF4J 输出内部日志，默认关闭。通过 Builder 开启：

```java
DDM ddm = DDM.builder(client)
    .enableLogging(true)
    .logLevel(Level.DEBUG)    // TRACE / DEBUG / INFO / WARN / ERROR
    .build();
```

| 级别 | 输出内容 |
|------|----------|
| `ERROR` | 操作失败、重试耗尽 |
| `WARN` | 批量重试、部分失败 |
| `INFO` | 实体注册、建表、操作完成 |
| `DEBUG` | 操作参数、变更字段、查询条件 |
| `TRACE` | DynamoDB 原始响应、单个 chunk 结果 |

需要在运行时 classpath 中提供 SLF4J 实现（如 `slf4j-simple`、`logback-classic`、`log4j-slf4j2-impl`）。

---

## 性能

- 每个实体类的元数据只解析一次，缓存在 `ConcurrentHashMap` 中——运行时零反射开销
- 字段访问通过 `MethodHandle`——JIT 预热后接近直接调用的性能
- 转换器在注册时绑定——运行时零查找开销
- 批量和更新操作通过虚拟线程（Java 21+）并行执行
- 支持自定义 `batchExecutor`，适用于不希望使用虚拟线程的环境

## License

MIT

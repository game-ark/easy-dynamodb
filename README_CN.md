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
ddm.saveBatch(userList);                           // 批量保存
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

## 完整示例

以一个游戏数据表为例，演示所有核心功能。

### 1. 定义实体

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

    // ---- 主键 ----

    @DynamoDbPartitionKey                          // 分区键（必须）
    @DynamoDbAttribute("game_id")                  // 自定义 DynamoDB 属性名
    public String getGameId() { return gameId; }
    public void setGameId(String gameId) { this.gameId = gameId; }

    @DynamoDbSortKey                               // 排序键（可选）
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    // ---- GSI（全局二级索引）----

    @DynamoDbSecondaryPartitionKey(indexNames = "genre-rating-index")
    public String getGenre() { return genre; }
    public void setGenre(String genre) { this.genre = genre; }

    @DynamoDbSecondarySortKey(indexNames = "genre-rating-index")
    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }

    // ---- 普通字段 ----

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public Integer getPlayerCount() { return playerCount; }
    public void setPlayerCount(Integer playerCount) { this.playerCount = playerCount; }

    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }

    // ---- 忽略字段 ----

    @DynamoDbIgnore                                // 不写入 DynamoDB
    public String getInternalNote() { return internalNote; }
    public void setInternalNote(String internalNote) { this.internalNote = internalNote; }
}
```

### 2. 初始化 DDM

```java
import com.jojo.framework.easydynamodb.DDM;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

DynamoDbClient client = DynamoDbClient.create();

// 方式一：快速创建（全部默认配置）
DDM ddm = DDM.create(client);

// 方式二：Builder 自定义配置
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")              // 所有表名加前缀 → "prod_game"
    .autoCreateTable(true)             // 首次 save 时自动建表（默认关闭）
    .register(Game.class)              // 预注册实体（也可以不注册，首次操作时自动注册）
    .build();
```

### 3. 保存

```java
Game game = new Game();
game.setGameId("zelda-001");
game.setVersion("v1.0");
game.setTitle("塞尔达传说");
game.setGenre("RPG");
game.setRating(9.8);
game.setPlayerCount(1);
game.setTags(List.of("adventure", "openworld"));

// 保存单条（null 字段自动跳过，不写入 DynamoDB）
ddm.save(game);

// 批量保存（超过 25 条自动分批，并行执行）
ddm.saveBatch(gameList);
```

### 4. 获取

```java
// 仅分区键
Game game = ddm.get(Game.class, "zelda-001");

// 分区键 + 排序键
Game game = ddm.get(Game.class, "zelda-001", "v1.0");

// 不存在返回 null
Game notFound = ddm.get(Game.class, "xxx", "v0");  // → null

// 批量获取（超过 100 条自动分批，并行执行）
List<Game> games = ddm.getBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));
```

### 5. 部分更新

```java
Game game = new Game();
game.setGameId("zelda-001");
game.setVersion("v1.0");

// 只更新指定字段，其他字段不受影响
ddm.update(game, g -> {
    g.setTitle("塞尔达传说：王国之泪");
    g.setRating(9.9);
});

// 全量更新（所有非 null 非键字段）
game.setTitle("塞尔达传说：王国之泪");
game.setRating(9.9);
game.setPlayerCount(1);
ddm.updateAll(game);
```

> `update` 内部原理：创建一个只含主键的空对象 → 传给你的 lambda → 对比哪些字段被设值了 → 只发送这些字段的 UpdateExpression。空 lambda 不会发送任何请求。

---

## 注解一览

EasyDynamodb 直接复用 AWS Enhanced Client 注解，无需学习新体系。仅 `@DynamoTable` 和 `@DynamoConverter` 是库独有的。

| 注解 | 位置 | 作用 |
|------|------|------|
| `@DynamoTable("表名")` | 类 | 指定 DynamoDB 表名。省略参数则用类名。**库独有** |
| `@DynamoDbBean` | 类 | AWS 注解，也可作为实体标记（表名=类名） |
| `@DynamoDbPartitionKey` | getter | 分区键（必须有且仅有一个） |
| `@DynamoDbSortKey` | getter | 排序键（可选） |
| `@DynamoDbAttribute("名称")` | getter | 自定义 DynamoDB 属性名 |
| `@DynamoDbIgnore` | getter | 忽略该字段，不读写 DynamoDB |
| `@DynamoDbSecondaryPartitionKey(indexNames={"idx"})` | getter | GSI 分区键 |
| `@DynamoDbSecondarySortKey(indexNames={"idx"})` | getter | GSI 排序键 |
| `@DynamoConverter(XxxConverter.class)` | 字段 | 指定自定义类型转换器。**库独有** |

---

## 类型映射

内置 14 种 Java 类型的自动转换，无需手动处理。

| Java 类型 | DynamoDB 类型 | 说明 |
|-----------|--------------|------|
| `String` | S | |
| `Integer` `Long` `Float` `Double` `BigDecimal` | N | |
| `Boolean` | BOOL | |
| `byte[]` | B | 二进制 |
| `List<T>` | L | 递归转换元素 |
| `Set<String>` | SS | 字符串集合 |
| `Set<Integer>` `Set<Long>` 等 | NS | 数值集合 |
| `Map<String, T>` | M | 递归转换值 |
| 嵌套实体 | M | 递归使用实体元数据转换 |
| `Instant` | S | ISO-8601 |
| `LocalDateTime` | S | ISO-8601 |

---

## 自定义转换器

当内置类型不满足需求时，实现 `AttributeConverter<T>` 接口：

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

两种注册方式：

```java
// 方式一：字段级 — 注解指定
@DynamoConverter(StatusConverter.class)
private Status status;

// 方式二：全局 — Builder 注册（对该类型的所有字段生效）
DDM ddm = DDM.builder(client)
    .registerConverter(Status.class, new StatusConverter())
    .build();
```

---

## 自动建表

默认关闭，适用于开发/测试环境。首次 `save` 时若表不存在，自动根据实体注解创建。

```java
DDM ddm = DDM.builder(client)
    .autoCreateTable(true)
    .build();

ddm.save(game);  // 表不存在 → 自动创建 → 保存
```

自动建表行为：
- 计费模式：`PAY_PER_REQUEST`（按需）
- 从 `@DynamoDbPartitionKey` / `@DynamoDbSortKey` 推断主键
- 从 `@DynamoDbSecondaryPartitionKey` / `@DynamoDbSecondarySortKey` 推断 GSI（Projection = ALL）
- 等待表状态 ACTIVE 后再执行保存

> ⚠️ 生产环境建议关闭，通过 IaC 工具管理表结构。

---

## 表名前缀与自定义解析

### 统一前缀

```java
DDM ddm = DDM.builder(client)
    .tablePrefix("prod_")   // @DynamoTable("game") → 实际表名 "prod_game"
    .build();
```

### 自定义表名解析器

继承 `TableNameResolver`，重写 `resolve` 方法：

```java
import com.jojo.framework.easydynamodb.metadata.TableNameResolver;

public class TenantResolver extends TableNameResolver {
    private final String tenantId;

    public TenantResolver(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String resolve(String tableName, String prefix) {
        // tableName = 注解原始表名, prefix = Builder 配置的前缀
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

## 批量操作

自动分批、并行执行、指数退避重试。

```java
// 批量保存（每批 25 条）
ddm.saveBatch(gameList);

// 批量获取（每批 100 条）
List<Game> games = ddm.getBatch(Game.class, List.of(
    new KeyPair("zelda-001", "v1.0"),
    new KeyPair("mario-001", "v2.0")
));
```

- 超过单批上限自动拆分
- 多批并行发送
- 未处理项自动重试（指数退避，最多 3 次）
- 重试后仍有失败项 → 抛出 `DynamoBatchException`

---

## 异常体系

所有异常继承 `RuntimeException`，无需 try-catch。

```
RuntimeException
└── DynamoException                    // 基类，包装 AWS SDK 异常
    ├── DynamoConfigException          // 注解配置错误（缺少主键、无转换器等）
    ├── DynamoConversionException      // 类型转换失败（含字段名、源类型、目标类型）
    └── DynamoBatchException           // 批量操作部分失败（含失败项列表）
```

---

## 性能设计

- 元数据一次解析，`ConcurrentHashMap` 缓存，运行时零反射
- 字段读写使用 `MethodHandle`，JIT 后接近直接调用
- 转换器在注册阶段绑定到字段，运行时无查找开销
- 批量操作并行执行，充分利用 DynamoDB 分布式特性

---

## 项目结构

```
com.jojo.framework.easydynamodb
├── DDM.java                           // 核心入口（save/get/update/saveBatch/getBatch）
├── annotation/
│   ├── DynamoTable.java               // 表名注解
│   └── DynamoConverter.java           // 自定义转换器注解
├── metadata/
│   ├── MetadataRegistry.java          // 元数据注册中心（解析注解、缓存）
│   ├── EntityMetadata.java            // 实体元数据（表名、键、字段列表、GSI）
│   ├── FieldMetadata.java             // 字段元数据（属性名、MethodHandle、转换器）
│   ├── GsiMetadata.java               // GSI 元数据（索引名、分区键、排序键）
│   └── TableNameResolver.java         // 表名解析器（可继承自定义）
├── converter/
│   ├── AttributeConverter.java        // 转换器接口
│   ├── ConverterRegistry.java         // 转换器注册中心（内置 14 种类型）
│   └── builtin/                       // 内置转换器
│       ├── StringConverter
│       ├── NumberConverter            // Integer/Long/Double/Float/BigDecimal
│       ├── BooleanConverter
│       ├── BinaryConverter            // byte[]
│       ├── ListConverter              // List<T> → L
│       ├── SetConverter               // Set<String> → SS, Set<Number> → NS
│       ├── MapConverter               // Map<String,T> → M
│       ├── NestedEntityConverter      // 嵌套实体 → M
│       ├── InstantConverter           // Instant → S (ISO-8601)
│       └── LocalDateTimeConverter     // LocalDateTime → S (ISO-8601)
├── operation/
│   ├── SaveOperation.java            // PutItem + 自动建表
│   ├── GetOperation.java             // GetItem
│   ├── UpdateOperation.java          // UpdateItem（mutator 模式 + 全量更新）
│   ├── BatchOperation.java           // BatchWriteItem / BatchGetItem
│   └── TableCreateOperation.java     // CreateTable（含 GSI）
├── model/
│   └── KeyPair.java                  // 批量获取用的主键对
└── exception/
    ├── DynamoException.java
    ├── DynamoConfigException.java
    ├── DynamoConversionException.java
    └── DynamoBatchException.java
```

## License

MIT

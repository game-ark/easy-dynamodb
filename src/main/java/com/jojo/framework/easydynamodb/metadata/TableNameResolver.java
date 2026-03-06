package com.jojo.framework.easydynamodb.metadata;

/**
 * 表名解析器，负责根据注解表名和配置的前缀生成最终的 DynamoDB 表名。
 * <p>
 * 默认实现为 prefix + tableName。可通过继承并重写 {@link #resolve(String, String)}
 * 方法实现自定义的表名生成逻辑（如多租户、环境区分等）。
 *
 * <pre>{@code
 * // 自定义示例：多租户表名
 * public class TenantTableNameResolver extends TableNameResolver {
 *     private final String tenantId;
 *
 *     public TenantTableNameResolver(String tenantId) {
 *         this.tenantId = tenantId;
 *     }
 *
 *     @Override
 *     public String resolve(String tableName, String prefix) {
 *         return prefix + tenantId + "_" + tableName;
 *     }
 * }
 * }</pre>
 */
public class TableNameResolver {

    /**
     * 根据注解表名和配置的前缀，返回最终使用的 DynamoDB 表名。
     *
     * @param tableName 从注解解析出的原始表名（@DynamoTable 的 value 或类名）
     * @param prefix    通过 Builder 配置的表名前缀，未配置时为空字符串
     * @return 最终使用的 DynamoDB 表名
     */
    public String resolve(String tableName, String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return tableName;
        }
        return prefix + tableName;
    }
}

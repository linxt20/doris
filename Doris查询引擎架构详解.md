# Doris查询引擎架构详解

## 目录
1. [概述](#概述)
2. [逻辑计划生成](#逻辑计划生成)
3. [逻辑计划优化](#逻辑计划优化)
4. [分布式计划生成](#分布式计划生成)
5. [分布式计划优化](#分布式计划优化)
6. [分布式计划切分](#分布式计划切分)
7. [分布式计划分发](#分布式计划分发)
8. [单机计划切分（Pipeline）](#单机计划切分pipeline)
9. [阻塞算子的切分处理](#阻塞算子的切分处理)
10. [操作流切分与数据流切分](#操作流切分与数据流切分)
11. [切分规则详解](#切分规则详解)

---

## 概述

Doris查询引擎采用基于Cascades框架的优化器（Nereids），将查询处理分为多个阶段：
- 逻辑计划生成：将SQL解析为逻辑计划树
- 逻辑计划优化：基于规则的优化（RBO）和基于代价的优化（CBO）
- 物理计划生成：将逻辑计划转换为物理计划
- 分布式计划生成：将物理计划转换为PlanFragment树
- 分布式计划切分：根据数据分布和算子特性切分Fragment
- 分布式计划分发：将Fragment分发到不同的BE节点
- 单机计划切分（Pipeline）：在BE端将Fragment切分为Pipeline

---

## 逻辑计划生成

### 文件位置
- 主要文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/parser/LogicalPlanBuilder.java`
- 辅助文件：
  - `fe/fe-core/src/main/java/org/apache/doris/nereids/parser/LogicalPlanBuilderAssistant.java`
  - `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/logical/LogicalPlan.java`

### 核心函数
1. `LogicalPlanBuilder.build()`：构建逻辑计划的主入口
2. `LogicalPlanBuilderAssistant.visitQuery()`：访问查询语句并构建逻辑计划树
3. `LogicalPlanBuilderAssistant.visitSelect()`：处理SELECT子句
4. `LogicalPlanBuilderAssistant.visitFrom()`：处理FROM子句，构建表扫描节点
5. `LogicalPlanBuilderAssistant.visitWhere()`：处理WHERE子句，构建Filter节点
6. `LogicalPlanBuilderAssistant.visitGroupBy()`：处理GROUP BY子句，构建Aggregate节点
7. `LogicalPlanBuilderAssistant.visitOrderBy()`：处理ORDER BY子句，构建Sort节点

### 逻辑计划节点类型（LogicalPlan）

#### 关系节点（Relation）
- LogicalOlapScan：OLAP表扫描
- LogicalFileScan：文件扫描（Hive、Iceberg等）
- LogicalEsScan：Elasticsearch扫描
- LogicalJdbcScan：JDBC表扫描
- LogicalOdbcScan：ODBC表扫描
- LogicalSchemaScan：Schema扫描
- LogicalTVFRelation：表值函数关系
- LogicalOneRowRelation：单行关系（常量查询）

#### 一元算子（Unary）
- LogicalFilter：过滤
- LogicalProject：投影
- LogicalSort：排序
- LogicalAggregate：聚合
- LogicalWindow：窗口函数
- LogicalLimit：限制
- LogicalPartitionTopN：分区TopN
- LogicalRepeat：重复（GROUPING SETS）
- LogicalGenerate：生成（LATERAL VIEW）
- LogicalLazyMaterialize：延迟物化

#### 二元算子（Binary）
- LogicalJoin：连接
  - LogicalHashJoin：哈希连接
  - LogicalNestedLoopJoin：嵌套循环连接
- LogicalUsingJoin：USING连接

#### 集合操作（Set Operation）
- LogicalUnion：并集
- LogicalIntersect：交集
- LogicalExcept：差集

#### 其他节点
- LogicalCTEAnchor：CTE锚点
- LogicalCTEProducer：CTE生产者
- LogicalCTEConsumer：CTE消费者
- LogicalSubQueryAlias：子查询别名
- LogicalView：视图
- LogicalResultSink：结果输出
- LogicalSqlCache：SQL缓存

### 执行流程
```
SQL语句 
  → Parser解析 
  → LogicalPlanBuilder.build() 
  → 构建LogicalPlan树
```

---

## 逻辑计划优化

### 文件位置
- 主要文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/executor/Optimizer.java`
- 优化器入口：`fe/fe-core/src/main/java/org/apache/doris/nereids/NereidsPlanner.java`

### 核心函数
1. `NereidsPlanner.planWithLock()`：计划生成的主入口
2. `NereidsPlanner.analyze()`：逻辑计划分析阶段
   - 函数：`cascadesContext.newAnalyzer().analyze()`
3. `NereidsPlanner.rewrite()`：基于规则的优化（RBO）
   - 函数：`Rewriter.getWholeTreeRewriter(cascadesContext).execute()`
4. `NereidsPlanner.optimize()`：基于代价的优化（CBO）
   - 函数：`new Optimizer(cascadesContext).execute()`
5. `Optimizer.execute()`：执行CBO优化
   - 包含统计信息推导和计划枚举

### 优化阶段

#### 1. 分析阶段（Analysis）
- 目的：解析列引用、表引用、函数调用等
- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/analyzer/`
- 关键函数：`Analyzer.analyze()`

#### 2. 基于规则的优化（RBO）
- 目的：应用启发式规则优化计划
- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/executor/Rewriter.java`
- 关键函数：`Rewriter.getWholeTreeRewriter().execute()`
- 优化规则示例：
  - 谓词下推（PredicatePushDown）
  - 列裁剪（ColumnPruning）
  - 常量折叠（ConstantFolding）
  - 子查询去关联（UncorrelatedSubquery）

#### 3. 基于代价的优化（CBO）
- 目的：基于统计信息和代价模型选择最优计划
- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/executor/Optimizer.java`
- 关键函数：
  - `Optimizer.execute()`：主优化流程
  - `DeriveStatsJob`：统计信息推导
  - `OptimizeGroupJob`：组优化
  - `JoinOrderJob`：连接顺序优化（DPHyp算法）

### 优化流程
```
逻辑计划 
  → analyze() [分析] 
  → rewrite() [RBO优化] 
  → preMaterializedViewRewrite() [物化视图重写] 
  → optimize() [CBO优化] 
  → chooseBestPlan() [选择最优计划]
```

---

## 分布式计划生成

### 文件位置
- 主要文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 入口函数：`NereidsPlanner.splitFragments()`

### 核心函数
1. `NereidsPlanner.splitFragments()`：将物理计划切分为Fragment
2. `PhysicalPlanTranslator.translatePlan()`：翻译物理计划为PlanFragment树
3. `PhysicalPlanTranslator.visitPhysicalXXX()`：访问各种物理算子并转换为PlanNode

### 物理计划切分为Fragment的详细处理

#### 切分位置

物理计划切分为Fragment发生在以下位置：

1. 入口函数：`NereidsPlanner.splitFragments()`
   - 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/NereidsPlanner.java`
   - 行号：577-605
   - 作用：创建`PhysicalPlanTranslator`并调用`translatePlan()`进行翻译

2. 翻译函数：`PhysicalPlanTranslator.translatePlan()`
   - 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
   - 行号：290-325
   - 作用：遍历物理计划树，通过访问者模式将每个节点转换为PlanNode，并构建PlanFragment树

3. 切分点：`PhysicalPlanTranslator.visitPhysicalDistribute()`
   - 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
   - 行号：332-439
   - 作用：这是Fragment切分的核心位置，每个`PhysicalDistribute`节点都会创建一个新的Fragment

#### 切分依据

Fragment切分遵循以下规则：

##### 1. PhysicalDistribute节点是切分点

- 规则：每个`PhysicalDistribute`节点都会触发Fragment切分
- 实现：在`visitPhysicalDistribute()`方法中：
  1. 先访问上游节点（`distribute.child()`），创建上游Fragment
  2. 创建`ExchangeNode`，作为上游Fragment的输出和下游Fragment的输入
  3. 创建下游Fragment，以`ExchangeNode`为根节点
  4. 设置上游Fragment的Sink为`DataStreamSink`，指向`ExchangeNode`
  5. 设置下游Fragment的数据分区类型（`DataPartition`）

##### 2. PhysicalDistribute的插入时机和CBO优化处理

核心机制：基于分布偏好满足性检查，而非简单的Plan Node类型判断

`PhysicalDistribute`的插入机制是根据分布偏好是否满足来决定，而不是简单地遍历plan node来决定哪些一定插入、哪些一定不插入。具体流程如下：

1. 遍历Plan Node并检查属性偏好

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/jobs/cascades/CostAndEnforcerJob.java`
- 函数：`execute()`（行号：115-228）、`enforce()`（行号：299-335）
- 流程：
  1. `CostAndEnforcerJob`遍历plan node（`GroupExpression`）
  2. 对每个plan node，使用`RequestPropertyDeriver`推导出对子节点的属性偏好（`RequiredProperties`）
  3. 获取子节点的实际输出属性（`outputProperties`）
  4. 检查子节点的输出属性是否满足父节点的偏好

2. 分布和有序性满足性检查

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/properties/EnforceMissingPropertiesHelper.java`
- 函数：`enforceProperty()`（行号：59-77）
  
  系统会同时检查有序性和分布，根据四种不同的情况采用不同的处理策略：

###### 情况1：分布和有序性都满足
- 条件：`isSatisfyDistribution == true && isSatisfyOrder == true`
- 处理：直接返回`output`，不插入任何enforcer节点
- 代码位置：第62-64行
  ```java
  if (isSatisfyDistribution && isSatisfyOrder) {
      return output;
  }
  ```

###### 情况2：只缺失有序性（分布满足）
- 条件：`isSatisfyDistribution == true && isSatisfyOrder == false`
- 处理：调用`enforceLocalSort()`，插入LocalSort enforcer节点
- 代码位置：第69-70行
  ```java
  if (!isSatisfyOrder) {
      return enforceLocalSort(output, required);
  }
  ```
- 实现细节（第104-113行）：
  - 保持原有的`DistributionSpec`不变
  - 创建新的`PhysicalProperties`，包含原有的分布属性和新的有序性要求
  - 插入`LocalQuickSort` enforcer节点
  - 更新代价和属性映射

###### 情况3：只缺失分布（有序性满足）
- 条件：`isSatisfyDistribution == false && isSatisfyOrder == true`
- 处理：需要考虑有序性是否会被重新分布破坏，分为两种情况：

  3.1 如果required属性包含有序性要求：
  - 处理：调用`enforceDistributionButMeetSort()`（第72-74行）
  - 原因：重新分布数据会破坏原有的有序性，需要特殊处理
  - 实现机制（第86-91行）：
    ```java
    // 技巧：先消除原有属性，避免无限循环
    groupExpression.getOwnerGroup()
            .replaceBestPlanProperty(output, PhysicalProperties.ANY, ...);
    // 然后同时强制执行排序和分布
    return enforceSortAndDistribution(PhysicalProperties.ANY, request);
    ```
  - 说明：这是一个特殊处理，因为如果直接添加enforce sort可能导致无限循环，所以先消除原有属性再统一处理

  3.2 如果required属性不包含有序性要求：
  - 处理：直接调用`enforceDistribution()`（第76行）
  - 实现：插入`PhysicalDistribute` enforcer节点

###### 情况4：分布和有序性都缺失 ❌
- 条件：`isSatisfyDistribution == false && isSatisfyOrder == false`
- 处理：调用`enforceSortAndDistribution()`，同时处理排序和分布
- 说明：不同的执行顺序会影响代价和性能，系统会根据属性类型选择最优策略
- LocalSort vs GlobalSort：
  - LocalSort（`enforceLocalSort`，第104-113行）：
    - 在每个分区/节点内部进行排序
    - 保持原有的`DistributionSpec`不变
    - 适用于分布式场景，各节点独立排序
    - Enforcer类型：`LocalQuickSort`
  - GlobalSort（`enforceGlobalSort`，第93-102行）：
    - 全局排序，通常需要先GATHER数据
    - 保持原有的`DistributionSpec`不变
    - 适用于需要全局有序的场景
    - Enforcer类型：`GlobalQuickSort`
  - 使用场景：
    - 当分布是GATHER时，需要先LocalSort再GATHER，最后GlobalSort确保全局有序
    - 当分布是Hash时，先Distribution再LocalSort，各分区内部有序即可


#### 不同算子的切分规则（star）

文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/properties/RequestPropertyDeriver.java`

说明：本节列举所有PlanNode对子节点的分布偏好，以及当子节点输出不满足偏好时，会插入什么类型的`PhysicalDistribute`。

插入机制：当子节点的输出属性（`outputProperties`）不满足父节点的偏好（`requiredProperties`）时，会在子节点上插入`PhysicalDistribute` enforcer节点。

###### 1. PhysicalBlackholeSink（黑洞Sink）
- 对子节点的偏好：`PhysicalProperties.ANY`（任意分布）
- 代码位置：行号136-140
- 插入PhysicalDistribute的情况：不会插入，因为接受任意分布

###### 2. PhysicalOlapTableSink（OLAP表Sink）
- 对子节点的偏好：
  - 如果`enableStrictConsistencyDml = false`：`PhysicalProperties.ANY`
  - 否则：`olapTableSink.getRequirePhysicalProperties()`（表要求的分布属性）
- 代码位置：行号143-150
- 插入PhysicalDistribute的情况：
  - 如果启用严格一致性DML且子节点输出不满足表要求的分布属性，会插入相应的`PhysicalDistribute`

###### 3. PhysicalHiveTableSink（Hive表Sink）
- 对子节点的偏好：
  - 如果`enableStrictConsistencyDml = false`：`PhysicalProperties.ANY`
  - 否则：`hiveTableSink.getRequirePhysicalProperties()`
- 代码位置：行号153-160
- 插入PhysicalDistribute的情况：同PhysicalOlapTableSink

###### 4. PhysicalIcebergTableSink（Iceberg表Sink）
- 对子节点的偏好：
  - 如果`enableStrictConsistencyDml = false`：`PhysicalProperties.ANY`
  - 否则：`icebergTableSink.getRequirePhysicalProperties()`
- 代码位置：行号163-171
- 插入PhysicalDistribute的情况：同PhysicalOlapTableSink

###### 5. PhysicalJdbcTableSink（JDBC表Sink）
- 对子节点的偏好：`PhysicalProperties.GATHER`（必须GATHER到单节点）
- 代码位置：行号174-179
- 插入PhysicalDistribute的情况：
  - 如果子节点输出不是GATHER分布，会插入`PhysicalDistribute(GATHER)`

###### 6. PhysicalDictionarySink（字典Sink）
- 对子节点的偏好：`dictionarySink.getRequirePhysicalProperties()`
- 代码位置：行号182-186
- 插入PhysicalDistribute的情况：如果子节点输出不满足要求的分布属性，会插入相应的`PhysicalDistribute`

###### 7. PhysicalResultSink（结果Sink）
- 对子节点的偏好：
  - 如果`enableParallelResultSink = true`且不是短路查询：`PhysicalProperties.ANY`
  - 否则：`PhysicalProperties.GATHER`
- 代码位置：行号189-197
- 插入PhysicalDistribute的情况：
  - 如果启用并行结果Sink且不是短路查询：不会插入
  - 否则：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`

###### 8. PhysicalDeferMaterializeResultSink（延迟物化结果Sink）
- 对子节点的偏好：`PhysicalProperties.GATHER`
- 代码位置：行号200-205
- 插入PhysicalDistribute的情况：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`

###### 9. PhysicalAssertNumRows（断言行数）
- 对子节点的偏好：`PhysicalProperties.GATHER`
- 代码位置：行号212-215
- 插入PhysicalDistribute的情况：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`

###### 10. PhysicalCTEAnchor（CTE锚点）
- 对子节点的偏好：
  - 第一个子节点：`PhysicalProperties.ANY`
  - 第二个子节点：`requestPropertyFromParent`（父节点的偏好）
- 代码位置：行号218-222
- 插入PhysicalDistribute的情况：
  - 第一个子节点：不会插入
  - 第二个子节点：如果输出不满足父节点偏好，会插入相应的`PhysicalDistribute`

###### 11. PhysicalHashJoin（Hash Join）
- 对子节点的偏好：根据Join类型和Hint决定
  - Broadcast Join（行号227-230）：
    - 左子节点：`PhysicalProperties.ANY`
    - 右子节点：`PhysicalProperties.REPLICATED`（广播）
  - Shuffle Join（行号232-235, 238-240, 530-545）：
    - 左子节点：`PhysicalProperties.createHash(leftJoinKeys, ShuffleType.REQUIRE)`
    - 右子节点：`PhysicalProperties.createHash(rightJoinKeys, ShuffleType.REQUIRE)`
    - 如果有数据倾斜信息，使用`ShuffleType.REQUIRE_EQUAL`
- 代码位置：行号225-248
- 插入PhysicalDistribute的情况：
  - Broadcast Join：如果右子节点输出不是REPLICATED，会插入`PhysicalDistribute(REPLICATED)`
  - Shuffle Join：如果左右子节点输出不满足按Join Key的Hash分布，会插入`PhysicalDistribute(HASH)`

###### 12. PhysicalLimit（Limit）
- 对子节点的偏好：
  - 如果`limit.isGlobal()`：`PhysicalProperties.GATHER`
  - 否则：`PhysicalProperties.ANY`
- 代码位置：行号251-258
- 插入PhysicalDistribute的情况：
  - 全局Limit：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`
  - 局部Limit：不会插入

###### 13. PhysicalNestedLoopJoin（嵌套循环Join）
- 对子节点的偏好：
  - 如果是Cross Join、Inner Join或Left Join（行号264-266）：
    - 左子节点：`PhysicalProperties.ANY`
    - 右子节点：`PhysicalProperties.REPLICATED`
  - 其他Join类型（行号268）：
    - 左右子节点：`PhysicalProperties.GATHER`
- 代码位置：行号261-271
- 插入PhysicalDistribute的情况：
  - Cross/Inner/Left Join：如果右子节点输出不是REPLICATED，会插入`PhysicalDistribute(REPLICATED)`
  - 其他Join：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`

###### 14. PhysicalSetOperation（集合操作：INTERSECT、EXCEPT）
- 对子节点的偏好：
  - 如果父节点要求Hash分布（行号277-281）：
    - 所有子节点：按父节点要求的Hash列进行分布
  - 否则（行号285-290）：
    - 所有子节点：按所有输出列进行Hash分布（`ShuffleType.EXECUTION_BUCKETED`）
- 代码位置：行号274-293
- 插入PhysicalDistribute的情况：如果子节点输出不满足要求的Hash分布，会插入`PhysicalDistribute(HASH)`

###### 15. PhysicalUnion（Union）
- 对子节点的偏好：
  - 默认（行号297-301）：所有子节点`PhysicalProperties.ANY`
  - 如果父节点要求Hash分布且没有常量表达式（行号304-312）：
    - 所有子节点：按父节点要求的Hash列进行分布
- 代码位置：行号296-316
- 插入PhysicalDistribute的情况：
  - 默认情况：不会插入
  - 如果父节点要求Hash分布：如果子节点输出不满足，会插入`PhysicalDistribute(HASH)`

###### 16. AbstractPhysicalSort（排序：包括QuickSort、TopN等）
- 对子节点的偏好：
  - 如果`sort.getSortPhase().isLocal()`（局部排序）：`PhysicalProperties.ANY`
  - 否则（全局排序）：`PhysicalProperties.GATHER`
- 代码位置：行号319-326
- 插入PhysicalDistribute的情况：
  - 局部排序：不会插入
  - 全局排序：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`

###### 17. PhysicalPartitionTopN（分区TopN）
- 对子节点的偏好：
  - 如果是两阶段LOCAL阶段（行号330-332）：`PhysicalProperties.ANY`
  - 否则（GLOBAL阶段，行号336-338）：`PhysicalProperties.createHash(partitionKeys, ShuffleType.REQUIRE)`
- 代码位置：行号329-341
- 插入PhysicalDistribute的情况：
  - LOCAL阶段：不会插入
  - GLOBAL阶段：如果子节点输出不满足按分区键的Hash分布，会插入`PhysicalDistribute(HASH)`

###### 18. PhysicalProject（投影）
- 对子节点的偏好：
  - 如果父节点不要求Hash分布（行号346-348）：`PhysicalProperties.ANY`（默认）
  - 如果父节点要求Hash分布且投影可以保持Hash分布（行号344-373）：
    - 选项1：`PhysicalProperties.ANY`
    - 选项2：`PhysicalProperties.createHash(projectedHashColumns)`（投影后的Hash列）
- 代码位置：行号344-374
- 插入PhysicalDistribute的情况：
  - 如果投影不能保持Hash分布：不会插入（使用ANY）
  - 如果可以保持：如果子节点输出不满足投影后的Hash分布，会插入`PhysicalDistribute(HASH)`

###### 19. PhysicalFilter（过滤）
- 对子节点的偏好：
  - 如果父节点不要求Hash分布（行号379-381）：`PhysicalProperties.ANY`（默认）
  - 如果父节点要求Hash分布（行号378-384）：
    - 选项1：`PhysicalProperties.ANY`
    - 选项2：`new PhysicalProperties(parentDist)`（保持父节点的Hash分布）
- 代码位置：行号377-385
- 插入PhysicalDistribute的情况：
  - 如果父节点不要求Hash分布：不会插入
  - 如果父节点要求Hash分布：如果子节点输出不满足，会插入`PhysicalDistribute(HASH)`

###### 20. PhysicalFileSink（文件Sink）
- 对子节点的偏好：`fileSink.requestProperties(connectContext)`（文件Sink要求的属性）
- 代码位置：行号388-391
- 插入PhysicalDistribute的情况：如果子节点输出不满足文件Sink要求的分布属性，会插入相应的`PhysicalDistribute`

###### 21. PhysicalWindow（窗口函数）
- 对子节点的偏好：
  - 如果没有分区键和排序键（行号416-417）：`PhysicalProperties.GATHER`
  - 如果只有排序键没有分区键（行号418-419）：`PhysicalProperties.GATHER.withOrderSpec(orderSpec)`
  - 如果有分区键（行号421-424）：
    - 分布：`PhysicalProperties.createHash(partitionKeys, ShuffleType.REQUIRE)`
    - 有序性：`OrderSpec`或`MustLocalSortOrderSpec`（如果数据倾斜）
- 代码位置：行号394-427
- 插入PhysicalDistribute的情况：
  - 没有分区键：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`
  - 有分区键：如果子节点输出不满足按分区键的Hash分布，会插入`PhysicalDistribute(HASH)`

###### 22. PhysicalHashAggregate（Hash聚合）
- 对子节点的偏好：
  - LOCAL阶段（行号432-434）：`PhysicalProperties.ANY`
  - GLOBAL阶段（行号435-467）：
    - 如果有分区表达式（行号436-439）：`PhysicalProperties.createHash(partitionExpressions, ShuffleType.REQUIRE)`
    - 如果没有group by（行号441-443）：`PhysicalProperties.GATHER`
    - 如果有group by（行号445-466）：
      - 如果父节点要求Hash分布且与group by有交集（行号452-464）：
        - 选项1：`PhysicalProperties.createHash(intersectKeys, ShuffleType.REQUIRE)`（交集）
        - 选项2：`PhysicalProperties.createHash(groupByKeys, ShuffleType.REQUIRE)`（完整group by）
      - 否则：`PhysicalProperties.createHash(groupByKeys, ShuffleType.REQUIRE)`
- 代码位置：行号430-470
- 插入PhysicalDistribute的情况：
  - LOCAL阶段：不会插入
  - GLOBAL阶段：
    - 有分区表达式：如果子节点输出不满足，会插入`PhysicalDistribute(HASH)`
    - 没有group by：如果子节点输出不是GATHER，会插入`PhysicalDistribute(GATHER)`
    - 有group by：如果子节点输出不满足按group by的Hash分布，会插入`PhysicalDistribute(HASH)`

##### 总结表格

| PlanNode | 对子节点的分布偏好 | 插入PhysicalDistribute的情况 |
|----------|------------------|---------------------------|
| PhysicalBlackholeSink | ANY | 不插入 |
| PhysicalOlapTableSink | ANY或表要求的分布 | 启用严格一致性DML且不满足时插入 |
| PhysicalHiveTableSink | ANY或表要求的分布 | 启用严格一致性DML且不满足时插入 |
| PhysicalIcebergTableSink | ANY或表要求的分布 | 启用严格一致性DML且不满足时插入 |
| PhysicalJdbcTableSink | GATHER | 不是GATHER时插入GATHER |
| PhysicalDictionarySink | Sink要求的分布 | 不满足时插入 |
| PhysicalResultSink | ANY或GATHER | 启用并行时ANY不插入，否则GATHER不满足时插入 |
| PhysicalDeferMaterializeResultSink | GATHER | 不是GATHER时插入GATHER |
| PhysicalAssertNumRows | GATHER | 不是GATHER时插入GATHER |
| PhysicalCTEAnchor | ANY + 父节点偏好 | 第二个子节点不满足父节点偏好时插入 |
| PhysicalHashJoin | ANY+REPLICATED或HASH+HASH | Broadcast时右子节点不满足REPLICATED插入，Shuffle时左右子节点不满足HASH插入 |
| PhysicalLimit | GATHER或ANY | 全局Limit且不是GATHER时插入GATHER |
| PhysicalNestedLoopJoin | ANY+REPLICATED或GATHER+GATHER | Cross/Inner/Left Join时右子节点不满足REPLICATED插入，其他Join时子节点不满足GATHER插入 |
| PhysicalSetOperation | HASH（所有列或父节点要求） | 不满足HASH分布时插入HASH |
| PhysicalUnion | ANY或HASH | 父节点要求HASH且不满足时插入HASH |
| AbstractPhysicalSort | ANY或GATHER | 全局排序且不是GATHER时插入GATHER |
| PhysicalPartitionTopN | ANY或HASH | GLOBAL阶段且不满足按分区键HASH时插入HASH |
| PhysicalProject | ANY或HASH | 可以保持HASH分布且不满足时插入HASH |
| PhysicalFilter | ANY或HASH | 父节点要求HASH且不满足时插入HASH |
| PhysicalFileSink | 文件Sink要求的分布 | 不满足时插入 |
| PhysicalWindow | GATHER或HASH | 没有分区键时GATHER不满足插入GATHER，有分区键时HASH不满足插入HASH |
| PhysicalHashAggregate | ANY或GATHER或HASH | LOCAL阶段不插入，GLOBAL阶段根据情况插入GATHER或HASH |

###### 3.1 Aggregate算子的切分

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalHashAggregate()`（行号：1121-1230）
- 规则：
  - 如果Aggregate需要GATHER分布，优化器会在Aggregate之前插入`PhysicalDistribute(GATHER)`
  - 如果Aggregate是分区聚合，不需要切分，在各自Fragment内聚合
  - 两阶段聚合：
    - LOCAL阶段：不插入ExchangeNode，在各自Fragment内聚合
    - GLOBAL阶段：如果数据分布不满足，会插入`PhysicalDistribute`，触发Fragment切分

###### 3.2 Join算子的切分

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalHashJoin()`（行号：1538-1720）、`connectJoinNode()`（行号：2908-2918）
- 规则：
  - Hash Join（PARTITIONED）：优化器会在左右两端都插入`PhysicalDistribute(HASH_PARTITIONED)`，按Join Key进行哈希分区
  - Broadcast Join：优化器会在小表端插入`PhysicalDistribute(UNPARTITIONED)`，进行广播
  - Colocate Join：不需要插入`PhysicalDistribute`，利用数据本地性，不切分Fragment
  - Bucket Shuffle Join：不需要插入`PhysicalDistribute`，利用Bucket分布，不切分Fragment
  - Join节点的Fragment合并：`connectJoinNode()`会将左右子Fragment合并到左Fragment中

###### 3.3 Sort算子的切分

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalQuickSort()`（行号：2370-2398）、`visitPhysicalTopN()`（行号：2401-2463）
- 规则：
  - 全局排序（GATHER）：优化器会在Sort之前插入`PhysicalDistribute(GATHER)`，收集所有数据到一个节点
  - 分区排序：不需要切分，在各自Fragment内排序
  - 合并排序（MERGE）：
    - 如果已有ExchangeNode（由`PhysicalDistribute`创建），将SortInfo推送到ExchangeNode
    - 如果没有ExchangeNode，说明数据分布已满足，忽略MERGE阶段

### Fragment到FragmentInstance的处理

#### 处理位置

Fragment到FragmentInstance的转换发生在`Coordinator`中：

- 入口函数：`Coordinator.computeFragmentExecParams()`
  - 文件：`fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java`
  - 行号：1335-1472
  - 作用：为每个Fragment创建`FragmentExecParams`，包含该Fragment的所有`FInstanceExecParam`

#### FragmentInstance数量和位置的确定

FragmentInstance的数量和位置由`computeFragmentHosts()`方法决定：

- 文件：`fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java`
- 函数：`computeFragmentHosts()`（行号：1728-1979）
- 处理顺序：自底向上（从叶子Fragment到根Fragment）

##### 1. Scan Fragment的Instance分配

规则：根据数据分布分配到包含数据的BE节点

- 处理逻辑（行号：1881-1979）：
  1. 获取Scan节点的数据分布信息（`scanRangeAssignment`）
  2. 对于每个包含数据的BE节点，创建`FInstanceExecParam`
  3. 考虑并行度（`parallelExecInstanceNum`）：
     - 如果`parallelExecInstanceNum > 1`，可以在同一BE上创建多个Instance
     - Instance数量不超过Tablet数量
  4. 分配ScanRange到各个Instance

- 代码位置：
  ```java
  for (Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : 
       fragmentExecParamsMap.get(fragment.getFragmentId()).scanRangeAssignment.entrySet()) {
      TNetworkAddress key = entry.getKey();  // BE地址
      // 为每个BE创建Instance
      FInstanceExecParam instanceParam = new FInstanceExecParam(null, key, params);
      params.instanceExecParams.add(instanceParam);
  }
  ```

##### 2. 非Scan Fragment的Instance分配

规则：继承上游Fragment的hosts

- 处理逻辑（行号：1823-1878）：
  1. 找到输入Fragment（`inputFragment`）
  2. 继承输入Fragment的hosts：
     - 如果设置了`exchangeInstanceParallel`，可能只选择部分hosts
     - 否则，继承所有输入Fragment的hosts
  3. 为每个host创建`FInstanceExecParam`

- 代码位置：
  ```java
  // 找到输入Fragment
  int maxParallelFragmentIndex = findMaxParallelFragmentIndex(fragment);
  PlanFragmentId inputFragmentId = fragment.getChild(maxParallelFragmentIndex).getFragmentId();
  
  // 继承输入Fragment的hosts
  for (FInstanceExecParam execParams : 
       fragmentExecParamsMap.get(inputFragmentId).instanceExecParams) {
      FInstanceExecParam instanceParam = new FInstanceExecParam(null, execParams.host, params);
      params.instanceExecParams.add(instanceParam);
  }
  ```

##### 3. UNPARTITIONED Fragment的Instance分配

规则：只创建一个Instance，分配到单个BE

- 处理逻辑（行号：1765-1809）：
  1. 选择一个BE节点（通过`SimpleScheduler.getHost()`）
  2. 创建一个`FInstanceExecParam`
  3. 如果`parallelExecNum > 1`且使用序列化源，在同一BE上创建多个Instance

##### 4. 特殊Fragment的Instance分配

- DictionarySink Fragment（行号：1738-1762）：
  - 分配到所有BE节点（或指定的部分BE节点）
  - Instance数量等于BE数量

- Colocate Join Fragment（行号：1883-1887）：
  - 根据Colocate信息分配到对应的BE节点

- Bucket Shuffle Join Fragment（行号：1888-1890）：
  - 根据Bucket分布分配到对应的BE节点

#### FragmentInstance ID的生成

- 文件：`fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java`
- 函数：`computeFragmentExecParams()`（行号：1339-1362）
- 规则：
  - 每个Instance都有唯一的`TUniqueId`
  - ID生成：`instanceId.lo = queryId.lo + instanceIds.size() + 1`
  - 保证全局唯一性


## 单机计划切分（Pipeline）

### 核心函数
1. `PipelineFragmentContext::_build_pipelines()`：构建Pipeline树
2. `PipelineFragmentContext::_create_tree_helper()`：递归创建算子树
3. `PipelineFragmentContext::_create_operator()`：创建单个算子
4. `PipelineFragmentContext::_plan_local_exchange()`：规划LocalExchange
5. `PipelineFragmentContext::_add_local_exchange_impl()`：添加LocalExchange

### Pipeline切分的处理顺序

重要结论：FragmentInstance是先切分为Pipeline，然后再插入LocalExchangeNode的。

处理顺序：
1. 第一阶段：构建Pipeline树（`_build_pipelines()`，行号：634-650）
   - 调用`_create_tree_helper()`递归创建算子树
   - 在创建阻塞算子时，会创建新的Pipeline（在`_create_operator()`中）

2. 第二阶段：插入LocalExchange（`_plan_local_exchange()`，行号：936-997）
   - 遍历所有Pipeline，检查数据分布需求
   - 当数据分布不匹配时，插入LocalExchange并切分Pipeline

### Limit算子的阻塞特性

Limit算子不是阻塞算子，不需要单独切分Pipeline。

原因：
1. 流式处理：Limit算子可以流式处理数据，不需要等待所有输入数据
2. 早期终止：当达到limit数量时，可以立即停止处理
3. 实现方式：Limit通常作为非阻塞算子实现，不会阻塞Pipeline

代码证据：
- Limit在FE端被转换为TopN（如果与Sort结合）或保留为Limit
- 在BE端，Limit通常作为非阻塞算子处理，不会触发Pipeline切分
- 如果Limit单独出现，它会被添加到当前Pipeline中，不会创建新的Pipeline

### 所有Node类型的Pipeline切分处理逻辑（star）

文件位置：`be/src/pipeline/pipeline_fragment_context.cpp`  
函数：`_create_operator()`（行号：1185-1658）

以下详细说明每个Node类型的Pipeline切分处理逻辑：

#### 1. Scan节点（不切分Pipeline）

##### 1.1 OLAP_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`OlapScanOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1198-1205
- 特殊处理：如果FE版本较旧且设置了`parallel_instances`，会设置算子为序列化算子（行号：1652-1655）

##### 1.2 GROUP_COMMIT_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`GroupCommitOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1206-1214
- 特殊处理：设置`is_group_commit_load`标志；如果FE版本较旧且设置了`parallel_instances`，会设置算子为序列化算子

##### 1.3 JDBC_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`JDBCScanOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1215-1227
- 前置条件：需要`config::enable_java_support`为true，否则返回错误
- 特殊处理：如果FE版本较旧且设置了`parallel_instances`，会设置算子为序列化算子

##### 1.4 FILE_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`FileScanOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1228-1234
- 特殊处理：如果FE版本较旧且设置了`parallel_instances`，会设置算子为序列化算子

##### 1.5 ES_SCAN_NODE / ES_HTTP_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`EsScanOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1235-1242
- 特殊处理：如果FE版本较旧且设置了`parallel_instances`，会设置算子为序列化算子

##### 1.6 DATA_GEN_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`DataGenSourceOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1627-1632
- 特殊处理：如果FE版本较旧且设置了`parallel_instances`，会设置算子为序列化算子

##### 1.7 SCHEMA_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`SchemaScanOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1633-1637

##### 1.8 META_SCAN_NODE
- 处理：不切分Pipeline
- 操作：创建`MetaScanOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1638-1642

#### 2. Exchange节点（不切分Pipeline）

##### 2.1 EXCHANGE_NODE
- 处理：不切分Pipeline
- 操作：创建`ExchangeSourceOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1243-1253
- 说明：ExchangeNode是Fragment之间的数据交换点，在Pipeline层级不需要切分
- 特殊处理：如果FE版本较旧且设置了`parallel_instances`，会设置算子为序列化算子

#### 3. Aggregate节点（有条件切分）

##### 3.1 AGGREGATION_NODE
- 文件位置：行号：1254-1376
- 切分条件：
  - 切分：非流式聚合（`!is_streaming_agg && !can_use_distinct_streaming_agg`）
  - 不切分：流式聚合（`is_streaming_agg`）或Distinct流式聚合（`can_use_distinct_streaming_agg`）

详细处理逻辑：

1. Distinct流式聚合（`can_use_distinct_streaming_agg`为true）：
   - 处理：不切分Pipeline
   - 操作：创建`DistinctStreamingAggOperatorX`，直接添加到当前Pipeline
   - 条件（行号：1291-1295）：
     - `aggregate_functions.empty()`（没有聚合函数）
     - 没有`agg_sort_info_by_group_key`
     - `enable_distinct_streaming_aggregation`为true
   - 代码位置：行号：1297-1316
   - 特殊处理：如果启用了查询缓存，会先创建Cache相关的Pipeline

2. 流式聚合（`is_streaming_agg`为true）：
   - 处理：不切分Pipeline
   - 操作：创建`StreamingAggOperatorX`，直接添加到当前Pipeline
   - 条件（行号：1288-1290）：
     - `use_streaming_preaggregation`为true
     - `grouping_exprs`不为空（有group by）
   - 代码位置：行号：1317-1331
   - 特殊处理：如果启用了查询缓存，会先创建Cache相关的Pipeline

3. 非流式聚合（`!is_streaming_agg && !can_use_distinct_streaming_agg`）：
   - 处理：切分Pipeline
   - 操作：
     - 创建`AggSourceOperatorX`（或`PartitionedAggSourceOperatorX`如果启用spill），添加到当前Pipeline
     - 创建新Pipeline
     - 在新Pipeline中添加`AggSinkOperatorX`（或`PartitionedAggSinkOperatorX`）
   - 代码位置：行号：1332-1375
   - Spill支持（行号：1286-1287）：
     - 如果启用spill且不是group by limit优化，使用`PartitionedAggSourceOperatorX`和`PartitionedAggSinkOperatorX`
   - 特殊处理：如果启用了查询缓存，会先创建Cache相关的Pipeline

#### 4. Join节点（无条件切分）

##### 4.1 HASH_JOIN_NODE
- 文件位置：行号：1378-1466
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`HashJoinProbeOperatorX`（或`PartitionedHashJoinProbeOperatorX`），添加到当前Pipeline（Probe端）
  - 创建新Pipeline（Build端）
  - 在新Pipeline中添加`HashJoinBuildSinkOperatorX`（或`PartitionedHashJoinSinkOperatorX`）

详细处理逻辑：

1. 启用Spill且非Broadcast Join（`enable_spill && !is_broadcast_join`）：
   - 创建`PartitionedHashJoinProbeOperatorX`和`PartitionedHashJoinSinkOperatorX`
   - 代码位置：行号：1382-1427

2. 其他情况：
   - 创建`HashJoinProbeOperatorX`和`HashJoinBuildSinkOperatorX`
   - 代码位置：行号：1427-1448

3. Broadcast Join共享Hash表（`is_broadcast_join && enable_share_hash_table_for_broadcast_join`）：
   - 创建共享的`HashJoinSharedState`
   - 代码位置：行号：1449-1462

##### 4.2 CROSS_JOIN_NODE
- 文件位置：行号：1467-1486
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`NestedLoopJoinProbeOperatorX`，添加到当前Pipeline（Probe端）
  - 创建新Pipeline（Build端）
  - 在新Pipeline中添加`NestedLoopJoinBuildSinkOperatorX`

#### 5. Union节点（无条件切分）

##### 5.1 UNION_NODE
- 文件位置：行号：1487-1509
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`UnionSourceOperatorX`，添加到当前Pipeline
  - 为每个子节点创建一个新Pipeline
  - 在每个新Pipeline中添加`UnionSinkOperatorX`
- 说明：Union有多个子节点，每个子节点都需要一个独立的Pipeline

#### 6. Sort节点（无条件切分）

##### 6.1 SORT_NODE
- 文件位置：行号：1511-1549
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`SortSourceOperatorX`（或`SpillSortSourceOperatorX`、`LocalMergeSortSourceOperatorX`），添加到当前Pipeline
  - 创建新Pipeline
  - 在新Pipeline中添加`SortSinkOperatorX`（或`SpillSortSinkOperatorX`）

详细处理逻辑：

1. 启用Spill且为FULL_SORT（`should_spill`为true）：
   - 创建`SpillSortSourceOperatorX`和`SpillSortSinkOperatorX`
   - 代码位置：行号：1516-1517, 1534-1537

2. 使用Local Merge（`use_local_merge`为true）：
   - 创建`LocalMergeSortSourceOperatorX`和`SortSinkOperatorX`
   - 代码位置：行号：1518-1520, 1539-1542

3. 其他情况：
   - 创建`SortSourceOperatorX`和`SortSinkOperatorX`
   - 代码位置：行号：1522-1523, 1539-1542

##### 6.2 PARTITION_SORT_NODE
- 文件位置：行号：1550-1567
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`PartitionSortSourceOperatorX`，添加到当前Pipeline
  - 创建新Pipeline
  - 在新Pipeline中添加`PartitionSortSinkOperatorX`

#### 7. Window节点（无条件切分）

##### 7.1 ANALYTIC_EVAL_NODE
- 文件位置：行号：1568-1588
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`AnalyticSourceOperatorX`，添加到当前Pipeline
  - 创建新Pipeline
  - 在新Pipeline中添加`AnalyticSinkOperatorX`

#### 8. 集合操作节点（无条件切分）

##### 8.1 INTERSECT_NODE
- 文件位置：行号：1595-1600, 1662-1693
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`SetSourceOperatorX<true>`，添加到当前Pipeline
  - 为每个子节点创建一个新Pipeline
  - 第一个子节点使用`SetSinkOperatorX`，其他子节点使用`SetProbeSinkOperatorX`
- 说明：Intersect有多个子节点，每个子节点都需要一个独立的Pipeline

##### 8.2 EXCEPT_NODE
- 文件位置：行号：1601-1606, 1662-1693
- 处理：无条件切分Pipeline
- 切分方式：
  - 创建`SetSourceOperatorX<false>`，添加到当前Pipeline
  - 为每个子节点创建一个新Pipeline
  - 第一个子节点使用`SetSinkOperatorX`，其他子节点使用`SetProbeSinkOperatorX`
- 说明：Except有多个子节点，每个子节点都需要一个独立的Pipeline

#### 9. 其他节点（不切分Pipeline）

##### 9.1 MATERIALIZATION_NODE
- 处理：不切分Pipeline
- 操作：创建`MaterializationOperator`，直接添加到当前Pipeline
- 代码位置：行号：1590-1594

##### 9.2 REPEAT_NODE
- 处理：不切分Pipeline
- 操作：创建`RepeatOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1607-1611

##### 9.3 TABLE_FUNCTION_NODE
- 处理：不切分Pipeline
- 操作：创建`TableFunctionOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1612-1616

##### 9.4 ASSERT_NUM_ROWS_NODE
- 处理：不切分Pipeline
- 操作：创建`AssertNumRowsOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1617-1621

##### 9.5 EMPTY_SET_NODE
- 处理：不切分Pipeline
- 操作：创建`EmptySetSourceOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1622-1626

##### 9.6 SELECT_NODE
- 处理：不切分Pipeline
- 操作：创建`SelectOperatorX`，直接添加到当前Pipeline
- 代码位置：行号：1643-1647



### Pipeline切分规则总结

#### 1. 阻塞算子切分（在`_create_operator()`中）

重要：并不是所有阻塞算子都会切分Pipeline，需要满足特定条件。

切分条件：

1. Sort算子：无条件切分
   - 文件：`be/src/pipeline/pipeline_fragment_context.cpp`
   - 行号：1511-1549
   - 逻辑：只要遇到`SORT_NODE`，就会切分Pipeline
   - 原因：Sort是阻塞算子，需要所有数据才能排序

2. HashAggregate算子：有条件切分
   - 文件：`be/src/pipeline/pipeline_fragment_context.cpp`
   - 行号：1254-1376
   - 切分条件：非流式聚合（`!is_streaming_agg`）
   - 不切分条件：流式聚合（`is_streaming_agg`）
     - 流式聚合判断条件（行号：1288-1290）：
       ```cpp
       const bool is_streaming_agg = tnode.agg_node.__isset.use_streaming_preaggregation &&
                                     tnode.agg_node.use_streaming_preaggregation &&
                                     !tnode.agg_node.grouping_exprs.empty();
       ```
     - 流式聚合可以流式处理数据，不需要等待所有数据，因此不需要切分Pipeline
   - 代码逻辑：
     - 如果`is_streaming_agg`为true：创建`StreamingAggOperatorX`，不切分Pipeline（行号：1317-1331）
     - 如果`is_streaming_agg`为false：创建`AggSourceOperatorX`和`AggSinkOperatorX`，切分Pipeline（行号：1332-1375）

3. HashJoin算子：无条件切分
   - 文件：`be/src/pipeline/pipeline_fragment_context.cpp`
   - 行号：1378-1466
   - 逻辑：只要遇到`HASH_JOIN_NODE`，就会切分Pipeline
   - 原因：HashJoin需要构建端数据才能进行连接，是阻塞算子

切分方式：
- 在创建Source Operator后，调用`add_pipeline(cur_pipe)`创建新Pipeline
- 在新Pipeline中添加Sink Operator
- 通过DAG建立Pipeline之间的依赖关系

总结：
- 无条件切分：Sort、HashJoin、Window
- 有条件切分：Aggregate（只有非流式聚合才切分）
- 切分原因：阻塞算子需要等待所有数据，切分Pipeline可以提高并行度，避免阻塞整个Pipeline

#### 2. 数据分布变化切分（在`_plan_local_exchange()`中）
当数据分布需求发生变化时，需要插入LocalExchange并切分Pipeline：
- HASH_SHUFFLE：需要哈希分布
- BUCKET_HASH_SHUFFLE：需要Bucket哈希分布
- PASSTHROUGH：需要均匀分布
- BROADCAST：需要广播
- PASS_TO_ONE：需要传递到一个节点

切分方式（行号：894-934）：
- 调用`_add_local_exchange()`检查是否需要LocalExchange
- 如果需要，调用`_add_local_exchange_impl()`插入LocalExchangeSink和LocalExchangeSource
- 将Pipeline切分为两个Pipeline

#### 3. 并行度不匹配切分（在`_add_local_exchange()`中）

**关键问题**：Doris在Pipeline切分时，**会考虑并行度不匹配的情况**，但不是所有并行度不匹配都会切分。

##### 3.1 并行度检查机制

**文件**：`be/src/pipeline/pipeline_fragment_context.cpp`
**函数**：`_add_local_exchange()`（行号：894-934）

**并行度相关字段**：
- `cur_pipe->num_tasks()`：当前Pipeline的并行度（任务数）
- `cur_pipe->num_tasks_of_parent()`：父Pipeline的并行度（任务数）
- `new_pip->num_tasks()`：新Pipeline的并行度（任务数）

**检查逻辑**（行号：899-901）：
```cpp
if (_num_instances <= 1 || cur_pipe->num_tasks_of_parent() <= 1) {
    return Status::OK();  // 如果父Pipeline并行度<=1，不插入LocalExchange
}
```

**说明**：
- 如果FragmentInstance数量<=1或父Pipeline并行度<=1，不会插入LocalExchange
- 这意味着**只有在多实例且父Pipeline并行度>1的情况下，才会考虑并行度不匹配**

##### 3.2 并行度不匹配的特殊处理

**场景**：上游Pipeline并行度>1，下游Pipeline并行度=1，且Sink有重操作

**代码位置**：行号：920-932
```cpp
// There are some local shuffles with relatively heavy operations on the sink.
// If the local sink concurrency is 1 and the local source concurrency is n, the sink becomes a bottleneck.
// Therefore, local passthrough is used to increase the concurrency of the sink.
// op -> local sink(1) -> local source (n)
// op -> local passthrough(1) -> local passthrough(n) ->  local sink(n) -> local source (n)
if (cur_pipe->num_tasks() > 1 && new_pip->num_tasks() == 1 &&
    Pipeline::heavy_operations_on_the_sink(data_distribution.distribution_type)) {
    RETURN_IF_ERROR(_add_local_exchange_impl(
            cast_set<int>(new_pip->operators().size()), pool, new_pip,
            add_pipeline(new_pip, pip_idx + 2), DataDistribution(ExchangeType::PASSTHROUGH),
            do_local_exchange, num_buckets, bucket_seq_to_instance_idx,
            shuffle_idx_to_instance_idx));
}
```

**处理逻辑**：
1. **条件检查**：
   - `cur_pipe->num_tasks() > 1`：上游Pipeline并行度>1
   - `new_pip->num_tasks() == 1`：下游Pipeline并行度=1
   - `Pipeline::heavy_operations_on_the_sink()`：Sink有重操作（HASH_SHUFFLE、BUCKET_HASH_SHUFFLE或ADAPTIVE_PASSTHROUGH）

2. **问题**：
   - 如果Local Sink的并发度是1，而Local Source的并发度是n，Sink会成为瓶颈
   - 因为Sink需要处理来自n个Source的数据，但只有1个并发度

3. **解决方案**：
   - 额外插入一个PASSTHROUGH类型的LocalExchange
   - 将Pipeline结构从：`op -> local sink(1) -> local source (n)`
   - 转换为：`op -> local passthrough(1) -> local passthrough(n) -> local sink(n) -> local source (n)`
   - 通过增加Passthrough层，将Sink的并发度从1提升到n

##### 3.3 重操作判断

**文件**：`be/src/pipeline/pipeline.h`
**函数**：`heavy_operations_on_the_sink()`（行号：79-82）

**判断逻辑**：
```cpp
static bool heavy_operations_on_the_sink(ExchangeType idx) {
    return idx == ExchangeType::HASH_SHUFFLE || 
           idx == ExchangeType::BUCKET_HASH_SHUFFLE ||
           idx == ExchangeType::ADAPTIVE_PASSTHROUGH;
}
```

**说明**：
- HASH_SHUFFLE和BUCKET_HASH_SHUFFLE需要在Sink端进行哈希计算和shuffle，是重操作
- ADAPTIVE_PASSTHROUGH也需要在Sink端进行自适应处理，是重操作
- 这些操作如果并发度不匹配，会导致Sink成为瓶颈

##### 3.4 总结：并行度不匹配的处理策略

| 场景 | 上游并行度 | 下游并行度 | 是否切分 | 处理方式 |
|------|-----------|-----------|---------|---------|
| **场景1** | >1 | =1 | 是（如果Sink有重操作） | 插入PASSTHROUGH LocalExchange增加Sink并发度 |
| **场景2** | =1 | >1 | 否 | 不切分（父Pipeline并行度<=1时不插入LocalExchange） |
| **场景3** | >1 | >1但不同 | 否（仅基于并行度） | 不单独因为并行度不同而切分，但可能因为数据分布需求而切分 |
| **场景4** | <=1 | 任意 | 否 | 父Pipeline并行度<=1时不插入LocalExchange |

**关键结论**：
1. **Doris会考虑并行度不匹配**，但**不是所有并行度不匹配都会切分**
2. **主要处理场景**：上游并行度>1，下游并行度=1，且Sink有重操作时，会额外插入PASSTHROUGH LocalExchange来增加Sink并发度
3. **其他并行度不匹配场景**：不会单独因为并行度不同而切分，但如果同时满足数据分布需求变化，会因数据分布需求而切分
4. **Fragment切分层面**：Fragment切分主要基于数据分布需求，不直接考虑并行度不匹配

#### 3. 序列化算子处理

序列化算子的含义：

序列化算子（Serial Operator）是指需要串行执行的算子，不能并行执行。

- 定义：`is_serial_operator()`返回true的算子
- 文件：`be/src/pipeline/exec/operator.h`
- 注释：`// Operators need to be executed serially. (e.g. finalized agg without key)`（行号：125）

为什么只能单线程执行：

1. 共享状态：序列化算子通常需要共享状态，多个线程同时访问会导致数据竞争
   - 例如：没有group by key的finalized aggregate（`finalized agg without key`）
   - 这种aggregate只有一个聚合结果，所有数据都需要写入同一个位置
   - 如果多线程并行执行，会导致数据竞争和结果错误

2. 实现限制：某些算子的实现方式决定了只能单线程执行
   - 例如：某些Scan算子可能因为资源限制或实现方式，只能单线程执行

3. 代码位置：
   - 文件：`be/src/pipeline/pipeline_fragment_context.cpp`
   - 行号：1654 - 当aggregate没有group by key时，会设置`op->set_serial_operator()`

序列化算子的影响：

- 序列化算子会影响Pipeline的并行度设置
- 在`need_to_local_exchange()`中会考虑序列化算子的影响（行号：40-44）
- 如果Pipeline中有序列化算子，可能不会插入LocalExchange来提高并行度


## 阻塞算子的切分处理

### 阻塞算子定义
阻塞算子是指需要接收所有输入数据后才能产生输出的算子，包括：
- Sort：需要所有数据才能排序
- HashAggregate：需要所有数据才能聚合
- HashJoin：需要构建端数据才能进行连接
- Window：需要窗口内所有数据才能计算

### 分布式计划层级的切分

#### 1. Sort算子
- 切分位置：分布式计划层级（FE端）
- 切分规则：
  - 如果Sort是全局排序（GATHER），在Sort之前插入ExchangeNode进行数据收集
  - 如果Sort是分区排序，不需要切分，在各自Fragment内排序
- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalTopN()`、`visitPhysicalQuickSort()`

#### 2. HashAggregate算子
- 切分位置：分布式计划层级（FE端）
- 切分规则：
  - 如果Aggregate是全局聚合（GATHER），在Aggregate之前插入ExchangeNode
  - 如果Aggregate是分区聚合，不需要切分，在各自Fragment内聚合
- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalHashAggregate()`

#### 3. HashJoin算子
- 切分位置：分布式计划层级（FE端）
- 切分规则：
  - Hash Join：在Join之前插入ExchangeNode进行数据重分布
  - Broadcast Join：在小表端插入ExchangeNode进行广播
  - Colocate Join：不需要切分，利用数据本地性
- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalHashJoin()`

### 单机计划层级（Pipeline）的切分

#### 1. Sort算子
- 切分位置：Pipeline层级（BE端）
- 切分规则：
  - Sort是阻塞算子，会在Sort前后切分Pipeline
  - Sort之前的数据可以并行处理
  - Sort之后的数据可以并行处理
- 文件：`be/src/pipeline/pipeline_fragment_context.cpp`
- 函数：`_create_tree_helper()`中检测`is_serial_operator()`

#### 2. HashAggregate算子
- 切分位置：Pipeline层级（BE端）
- 切分规则：
  - HashAggregate是阻塞算子，会在Aggregate前后切分Pipeline
  - 如果数据分布不匹配，会在Aggregate之前插入LocalExchange
- 文件：`be/src/pipeline/pipeline_fragment_context.cpp`
- 函数：`_create_tree_helper()`中检测阻塞算子

#### 3. HashJoin算子
- 切分位置：Pipeline层级（BE端）
- 切分规则：
  - HashJoin的Build端和Probe端会切分为不同的Pipeline
  - Build端Pipeline先执行，构建Hash表
  - Probe端Pipeline后执行，使用Hash表进行连接
- 文件：`be/src/pipeline/pipeline_fragment_context.cpp`
- 函数：`_create_tree_helper()`中处理Join节点

### 是否一定在阻塞算子之后切分？

答案：不一定

1. 分布式计划层级：
   - 如果阻塞算子需要全局数据（GATHER），会在之前插入ExchangeNode切分
   - 如果阻塞算子可以分区处理，不会切分

2. Pipeline层级：
   - 阻塞算子总是会切分Pipeline
   - 切分发生在阻塞算子的前后，而不是只在之后

## Partial/Final Agg和Sort的生成与ExchangeNode插入 

### Partial/Final Agg的生成

#### 生成规则
- 规则名称：`SplitAggWithoutDistinct`
- 文件位置：`fe/fe-core/src/main/java/org/apache/doris/nereids/rules/implementation/SplitAggWithoutDistinct.java`
- 规则类型：Implementation规则（CBO阶段）
- 函数：`splitTwoPhase()`

#### 生成时机
Partial/Final Agg是在添加ExchangeNode之前生成的

生成顺序：
1. 优化阶段（CBO）：`SplitAggWithoutDistinct`规则将`LogicalAggregate`转换为两阶段的`PhysicalHashAggregate`
   - 生成`PhysicalHashAggregate(LOCAL)`（Partial Agg）
   - 生成`PhysicalHashAggregate(GLOBAL)`（Final Agg），其子节点是LOCAL Agg
2. 翻译阶段：当GLOBAL Agg需要GATHER分布时，优化器会插入`PhysicalDistribute`
3. 翻译阶段：`visitPhysicalDistribute()`函数将`PhysicalDistribute`转换为`ExchangeNode`

### Partial/Final Sort的生成

#### 生成规则
- 规则名称：
  - `LogicalSortToPhysicalQuickSort`（用于ORDER BY）
  - `LogicalTopNToPhysicalTopN`（用于ORDER BY LIMIT）
- 文件位置：
  - `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/implementation/LogicalSortToPhysicalQuickSort.java`
  - `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/implementation/LogicalTopNToPhysicalTopN.java`
- 规则类型：Implementation规则（CBO阶段）
- 函数：`twoPhaseSort()`

#### 生成时机
Partial/Final Sort是在添加ExchangeNode之前生成的

生成顺序：
1. 优化阶段（CBO）：`LogicalSortToPhysicalQuickSort`或`LogicalTopNToPhysicalTopN`规则将`LogicalSort`/`LogicalTopN`转换为两阶段的`PhysicalQuickSort`/`PhysicalTopN`
   - 生成`PhysicalQuickSort(LOCAL_SORT)`（Partial Sort）
   - 生成`PhysicalQuickSort(MERGE_SORT)`（Final Sort），其子节点是LOCAL_SORT
2. 翻译阶段：当MERGE_SORT需要GATHER分布时，优化器会插入`PhysicalDistribute`
3. 翻译阶段：`visitPhysicalDistribute()`函数将`PhysicalDistribute`转换为`ExchangeNode`，或者`visitPhysicalQuickSort()`/`visitPhysicalTopN()`直接处理MERGE_SORT


## 核心概念详解

### 问题1：流式聚合 vs 非流式聚合

#### 流式聚合（Streaming Aggregation）

定义：流式聚合是一种可以增量处理数据的聚合方式，不需要等待所有输入数据，可以边接收边处理边输出。

特点：
1. 增量处理：每接收到一批数据，立即进行聚合处理
2. 流式输出：可以立即输出部分聚合结果，不需要等待所有数据
3. 非阻塞：不会阻塞Pipeline，可以与其他算子并行执行
4. 内存效率：使用Hash表维护聚合状态，可以及时输出结果释放内存

适用条件（行号：1288-1290）：
```cpp
const bool is_streaming_agg = tnode.agg_node.__isset.use_streaming_preaggregation &&
                              tnode.agg_node.use_streaming_preaggregation &&
                              !tnode.agg_node.grouping_exprs.empty();
```
- `use_streaming_preaggregation`为true
- 有group by表达式（`grouping_exprs`不为空）

实现方式：
- 使用`StreamingAggOperatorX`算子
- 维护一个Hash表，key是group by的列，value是聚合状态
- 每接收到一批数据，更新Hash表中的聚合状态
- 可以立即输出已完成的聚合结果

示例：
```sql
-- 流式聚合示例
SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
FROM orders
GROUP BY user_id
```

执行过程：
1. 接收到数据：`(user1, 100)`, `(user2, 200)`, `(user1, 150)`
2. 立即处理：
   - user1: cnt=2, total=250
   - user2: cnt=1, total=200
3. 可以立即输出部分结果，不需要等待所有数据

Pipeline处理：不切分Pipeline，直接添加到当前Pipeline中

#### 非流式聚合（Hash Aggregation）

定义：非流式聚合是一种需要等待所有输入数据才能完成聚合的方式，是阻塞算子。

特点：
1. 阻塞处理：需要接收所有输入数据后才能产生输出
2. 批量处理：所有数据收集完成后，统一进行聚合
3. 阻塞Pipeline：会阻塞Pipeline，需要切分Pipeline以提高并行度
4. 内存占用：需要存储所有数据，内存占用较大

适用条件：
- 不满足流式聚合的条件
- 或者流式聚合被禁用

实现方式：
- 使用`AggSinkOperatorX`和`AggSourceOperatorX`算子对
- Sink端：收集所有数据，进行聚合计算
- Source端：从SharedState读取聚合结果并输出
- 通过SharedState在Sink和Source之间传递数据

示例：
```sql
-- 非流式聚合示例（没有group by的聚合）
SELECT COUNT(*) as total_count, SUM(amount) as total_amount
FROM orders
```

执行过程：
1. Sink端：接收所有数据，进行聚合计算
2. 等待所有数据接收完成
3. Source端：从SharedState读取聚合结果并输出

Pipeline处理：切分Pipeline，分为Sink Pipeline和Source Pipeline

#### 对比总结

| 特性 | 流式聚合 | 非流式聚合 |
|------|---------|-----------|
| 处理方式 | 增量处理，边接收边处理 | 批量处理，等待所有数据 |
| 输出时机 | 可以立即输出部分结果 | 需要等待所有数据 |
| 阻塞特性 | 非阻塞 | 阻塞 |
| Pipeline切分 | 不切分 | 切分 |
| 内存占用 | 较小（可以及时输出） | 较大（需要存储所有数据） |
| 适用场景 | 有group by的聚合 | 没有group by或流式聚合不可用 |
| 实现算子 | `StreamingAggOperatorX` | `AggSinkOperatorX` + `AggSourceOperatorX` |

### 问题2：序列化算子（Serial Operator）

##### 3. 不同算子的切分规则（star）

###### 3.1 Aggregate算子的切分

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalHashAggregate()`（行号：1121-1230）
- 规则：
  - 如果Aggregate需要GATHER分布，优化器会在Aggregate之前插入`PhysicalDistribute(GATHER)`
  - 如果Aggregate是分区聚合，不需要切分，在各自Fragment内聚合
  - 两阶段聚合：
    - LOCAL阶段：不插入ExchangeNode，在各自Fragment内聚合
    - GLOBAL阶段：如果数据分布不满足，会插入`PhysicalDistribute`，触发Fragment切分

###### 3.2 Join算子的切分

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalHashJoin()`（行号：1538-1720）、`connectJoinNode()`（行号：2908-2918）
- 规则：
  - Hash Join（PARTITIONED）：优化器会在左右两端都插入`PhysicalDistribute(HASH_PARTITIONED)`，按Join Key进行哈希分区
  - Broadcast Join：优化器会在小表端插入`PhysicalDistribute(UNPARTITIONED)`，进行广播
  - Colocate Join：不需要插入`PhysicalDistribute`，利用数据本地性，不切分Fragment
  - Bucket Shuffle Join：不需要插入`PhysicalDistribute`，利用Bucket分布，不切分Fragment
  - Join节点的Fragment合并：`connectJoinNode()`会将左右子Fragment合并到左Fragment中

###### 3.3 Sort算子的切分

- 文件：`fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java`
- 函数：`visitPhysicalQuickSort()`（行号：2370-2398）、`visitPhysicalTopN()`（行号：2401-2463）
- 规则：
  - 全局排序（GATHER）：优化器会在Sort之前插入`PhysicalDistribute(GATHER)`，收集所有数据到一个节点
  - 分区排序：不需要切分，在各自Fragment内排序
  - 合并排序（MERGE）：
    - 如果已有ExchangeNode（由`PhysicalDistribute`创建），将SortInfo推送到ExchangeNode
    - 如果没有ExchangeNode，说明数据分布已满足，忽略MERGE阶段

#### 定义

序列化算子（Serial Operator）是指需要串行执行的算子，不能并行执行，只能单线程执行。

代码定义（`be/src/pipeline/exec/operator.h`，行号：125-126）：
```cpp
// Operators need to be executed serially. (e.g. finalized agg without key)
[[nodiscard]] virtual bool is_serial_operator() const { return _is_serial_operator; }
```

#### 为什么需要序列化算子

原因1：共享状态导致数据竞争

某些算子需要维护共享状态，多个线程同时访问会导致数据竞争和结果错误。

典型例子：没有group by key的finalized aggregate

```sql
-- 示例：没有group by的聚合
SELECT COUNT(*) as total, SUM(amount) as total_amount
FROM orders
```

问题：
- 这种聚合只有一个聚合结果（所有数据聚合到一个值）
- 所有数据都需要写入同一个内存位置
- 如果多线程并行执行，多个线程同时更新同一个聚合状态，会导致：
  - 数据竞争（Race Condition）
  - 结果错误（计数不准确、求和错误等）

解决方案：设置为序列化算子，只能单线程执行

代码位置（`be/src/pipeline/pipeline_fragment_context.cpp`，行号：1652-1655）：
```cpp
if (_params.__isset.parallel_instances && fe_with_old_version) {
    cur_pipe->set_num_tasks(_params.parallel_instances);
    op->set_serial_operator();  // 设置为序列化算子
}
```

原因2：实现限制

某些算子的实现方式决定了只能单线程执行：
- 资源限制：某些Scan算子可能因为资源限制（如文件句柄、网络连接）只能单线程执行
- 算法特性：某些算法本身不支持并行执行

#### 序列化算子的影响

1. 并行度限制：
   - 序列化算子所在的Pipeline只能单线程执行
   - 即使Pipeline中有其他可以并行的算子，也会被限制为单线程

2. LocalExchange优化：
   - 如果Pipeline中有序列化算子，可能不会插入LocalExchange来提高并行度
   - 代码位置：`be/src/pipeline/pipeline.cpp`，行号：62-68

3. 性能影响：
   - 序列化算子会成为性能瓶颈
   - 需要尽量避免或优化序列化算子的使用

#### 序列化算子示例

示例1：没有group by的聚合
```sql
SELECT COUNT(*) FROM orders;
```
- 所有数据聚合到一个结果
- 必须序列化执行，避免数据竞争

示例2：某些Scan算子
- 某些Scan算子可能因为资源限制（如JDBC连接）只能单线程执行
- 代码位置：`be/src/pipeline/pipeline_fragment_context.cpp`，行号：1215-1227

示例3：FE版本兼容
- 如果FE版本较旧且设置了`parallel_instances`，某些算子会被设置为序列化算子
- 代码位置：行号：1652-1655

### 问题3：Fragment vs Pipeline的相同点和不同点

#### 相同点

1. 都是执行单元：
   - Fragment是分布式执行的单元
   - Pipeline是单机执行的单元

2. 都通过切分提高并行度：
   - Fragment切分：在分布式层面提高并行度（跨节点）
   - Pipeline切分：在单机层面提高并行度（节点内）

3. 都有数据交换机制：
   - Fragment之间通过`ExchangeNode`交换数据
   - Pipeline之间通过`LocalExchangeNode`或`SharedState`交换数据

4. 都基于数据分布和算子特性切分：
   - Fragment切分：基于数据分布需求（GATHER、PARTITIONED等）
   - Pipeline切分：基于阻塞特性和数据分布需求

重要区别：虽然两者都涉及切分，但阻塞特性的切分必须在Pipeline阶段完成，不能在Fragment阶段完成，原因如下：

#### 不同点

| 特性 | Fragment | Pipeline |
|------|----------|----------|
| 层级 | 分布式计划层级 | 单机计划层级 |
| 处理位置 | FE端（Coordinator） | BE端（Backend） |
| 切分时机 | 查询计划生成时 | Fragment执行时 |
| 切分粒度 | 粗粒度（跨节点） | 细粒度（节点内） |
| 数据交换 | ExchangeNode（跨节点网络传输） | LocalExchangeNode（节点内内存传输）或SharedState |
| 切分依据 | 数据分布需求 | 阻塞特性和数据分布需求 |
| 并行度 | 跨节点并行（多个BE节点） | 节点内并行（多个线程） |
| 资源管理 | 跨节点资源调度 | 节点内资源调度 |
| 容错机制 | 节点级别容错 | 线程级别容错 |


### 为什么阻塞特性切分必须在Pipeline阶段完成？（star）

关键问题：为什么基于阻塞特性的切分不能安排在分布式计划切分出Fragment的时候，而要保留到FragmentInstance切分出Pipeline的阶段？

总结：

阻塞特性切分必须在Pipeline阶段完成，而不能在Fragment阶段完成，因为：

1. 信息可用性：Fragment切分时不知道执行节点和节点资源信息
2. 优化目标：阻塞算子切分是为了节点内并行度优化，不是跨节点优化
3. 执行时机：阻塞算子切分需要执行时的上下文信息
4. 数据传递：阻塞算子切分使用节点内内存共享，不需要跨节点考虑
5. 灵活性：在BE端可以根据实际资源情况动态调整切分策略

1. 关注点分离：
   - Fragment层级：关注分布式执行
     - 数据分布策略（GATHER、PARTITIONED、BROADCAST）
     - 节点选择和调度
     - 跨节点数据交换
   - Pipeline层级：关注单机执行
     - 阻塞算子处理
     - 节点内并行度优化
     - 内存和CPU资源利用

2. 执行时机不同：
   - Fragment切分：在FE端查询计划生成时完成
     - FE端有全局视图，可以做出最优的分布式决策
     - 可以提前规划数据分布和节点分配
   - Pipeline切分：在BE端Fragment执行时完成
     - BE端有本地资源信息（CPU、内存、负载）
     - 可以根据实际资源情况动态调整Pipeline并行度

3. 优化目标不同：
   - Fragment优化：
     - 最小化跨节点数据传输
     - 最大化数据本地性
     - 负载均衡
   - Pipeline优化：
     - 最大化节点内并行度
     - 最小化内存占用
     - 优化阻塞算子的执行

4. 灵活性：
   - 两层切分：可以在不同层级独立优化
     - FE端优化分布式策略
     - BE端优化单机执行
   - 单层切分：难以同时兼顾分布式和单机优化

5. 实现复杂度：
   - 两层切分：
     - Fragment切分：相对简单，主要考虑数据分布
     - Pipeline切分：相对复杂，需要考虑阻塞算子、数据分布、资源限制等
   - 单层切分：需要在FE端考虑所有因素，复杂度高

#### 执行流程示例（star）

```
SQL查询
  ↓
FE端：生成物理计划
  ↓
FE端：切分为Fragment（考虑数据分布）
  ├─ Fragment 0: Scan (BE1, BE2, BE3)
  ├─ Fragment 1: Aggregate (BE1, BE2, BE3)
  └─ Fragment 2: Sort (BE1)
  ↓
FE端：分发Fragment到BE节点
  ↓
BE端：接收Fragment
  ↓
BE端：切分为Pipeline（考虑阻塞特性和数据分布）
  ├─ Pipeline 0: Scan -> LocalExchange
  ├─ Pipeline 1: LocalExchange -> AggSink
  ├─ Pipeline 2: AggSource -> SortSink
  └─ Pipeline 3: SortSource -> Result
  ↓
BE端：执行Pipeline
```

#### 总结（star）

为什么需要两层切分：
1. 关注点分离：分布式执行 vs 单机执行
2. 执行时机：计划生成时 vs 执行时
3. 优化目标：跨节点优化 vs 节点内优化
4. 灵活性：可以在不同层级独立优化
5. 实现复杂度：分层实现更简单、更易维护

两层切分的优势：
- FE端专注于分布式优化（数据分布、节点选择）
- BE端专注于单机优化（并行度、资源利用）
- 可以在不同层级独立优化，互不干扰
- 实现更简单，维护更容易

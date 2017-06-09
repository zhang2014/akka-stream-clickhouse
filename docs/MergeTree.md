# MergeTree

![MergeTree](https://github.com/zhang2014/akka-stream-clickhouse/blob/master/resources/MergeTree.png?raw=true&a)

## 介绍

如图所示,对于MergeTree的所有写入的数据都将以块的形式被写入到Level-1中,随着时间及数据的不断新增,
Level-1中的数据不断的和其他Level中的数据合并后晋升到下一层Level中
而随着数据的Level不断下沉,数据不断被整理为对加载更为友好,规整,有序的数据
### Block
按列存储,按批量划分列的值(同时做为最小的读取与写入单元,该数据同时会被压缩)如:
```
┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
│ 1970-01-01 │       1 │ OnClick   │     3 │
│ 1970-01-02 │       1 │ OnClick   │     2 │
└────────────┴─────────┴───────────┴───────┘
┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
│ 1970-02-01 │       1 │ OnClick   │     3 │
└────────────┴─────────┴───────────┴───────┘
```
在上述表中,即存在四个列,分别为`eventDate`,`eventId`,`eventName`,`count`,其中每列对应两个Block(Block的划分取决于是否跨月,以及是否超出建表时
所指定的最大行数,默认推荐8192,可以通过建表时指明`Engine = MergeTree(eventDate, (eventId,eventName), 8192)`进行修改).针对`eventDate`列的
Block-1应该存在'1970-01-01','1970-01-02'数据,针对`eventDate`列Block-2应该存在'1970-02-01'字段。对于Insert语句而言,会将跨月的数据进行不同的Block进行划分
后在根据最大行数(8192)分割后存入磁盘中
### Part
`多个列的一个或多个Block` + `索引` 所组成的数据集,可通过`system.parts`查看当前的所有Part信息,其中Part的Name的命名规则为"`当前Part中包含的最小日期
_当前Part中包含的最大日期_当前Part中包含的最小BlockId_当前Part中包含的最大BlockId_当前Part所属的Level层级`"(对于Part而言是不能够跨月的)。
对于Clickhouse而言,数据是按照Part进行检索的,当Select到来是,通过主键定位到需要查询的Part,后将当前与主键相关的检索条件拿到primary.idx中进行搜索,确定
需要加载的Block列表后加载对应的Block
### Partition
多个Part所组成的数据集,可通过`system.partitions`查看当前所有的Partition信息,该数据为对相同月份的Part进行的逻辑划分,不存在物理结构
### MergeTreeTable
多个Part组成的数据集,可通过`system.tables`查看当前所有的table信息,该数据是所有的SQL最终操作的逻辑单位。
### DataMergeTask
后台合并任务,负责选择合适Part并对它们进行合并操作。对于合并操作针对于每种不同的MergeTree存在不同的合并策略,下面对各类MergeTree的Merge策略详细说明
#### MergeTree
最为简单的MergeTree类型,对于合并操作不做太多的操作,在保持合并后的索引顺序不变的情况下将`两个Part的相同列的Block合并到一个文件中`
#### AggaregatingMergeTree
最为复杂的MergeTree类型,针对AggregatingMergeTree,Clickhouse提供了新的数据类型:`AggregatedFunction(aggFunctionName,dataType)`用于在数据合并
期间进行一些复杂的合并策略,其中`dataType`用来填写实际本列的数据类型,`aggFunctionName`用来填写具体的合并策略,如Sum,Max,Min等,这样在两个Part数据进行
合并时AggregationMergeTree将对非索引,主键的AggaregatedFunction类型的字段按照aggFunctionName提供的方式进行聚合。但因为字段类型被写成了AggregatedFunction
类型,所以需要改写Insert与Select语句,Insert语句需要改写为:
```SQL
INSERT INTO test_table SELECT '1970-01-01' as eventDate,1 as eventId,'OnClick' as eventName,sumState(3) as count
INSERT INTO test_table SELECT '1970-01-02' as eventDate,1 as eventId,'OnClick' as eventName,sumState(2) as count
INSERT INTO test_table SELECT '1970-02-01' as eventDate,1 as eventId,'OnClick' as eventName,sumState(3) as count
```
Select 语句需要改写为:
```SQL
SELECT sumMerge(count),eventId FROM test_table GROUP BY eventId
```
#### CollapsingMergeTree
#### SummingMergeTree
#### UnsortedMergeTree
#### ReplacingMergeTree
#### GraphiteMergeTree

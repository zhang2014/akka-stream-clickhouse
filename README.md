# akka-stream-clickhouse

项目主要用于学习研究[ClickHouse](https://clickhouse.yandex/)([Github](https://github.com/yandex/ClickHouse))使用,本项目期望实现可以将Clickhouse中(Table,DataPart,Column)任一角色的数据进行读取后通过Akka-Stream进行转换后写回Clickhouse中,如下图:

![Overview](https://github.com/zhang2014/akka-stream-clickhouse/blob/master/overview.png?raw=true)


完成这一目标的主要的TODO如下(具体DataPart,Column说明见后文说明):
- [X] ColumnSource,用于读取位于DataPart下的Column文件的数据
- [X] DataPartSource,用于读取位于Table下的DataPart文件夹的数据内容
- [X] TableSource,用于读取位于Database下的Table文件夹的数据内容
- [ ] ColumnSink,用于将Akka-Stream中的数据写回到DataPart下的Column文件中
- [ ] DataPartSink,用于将Akka-Stream中的数据写回到Table下的DataPart文件夹中
- [ ] TableSink,用于将Akka-Stream中的数据写回到Database下的Table文件夹中

## 使用说明
### 背景说明
通过Clickhouse查询到如下结果:
```
> SELECT * FROM  default.test_table_1;

      ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
      │ 1980-01-01 │       1 │ OnClick   │     3 │
      └────────────┴─────────┴───────────┴───────┘
      ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
      │ 0000-00-00 │       1 │ OnClick   │     3 │
      └────────────┴─────────┴───────────┴───────┘
     
>  SELECT * FROM system.parts WHERE database = 'default' AND table = 'test_table_1'

┌─partition─┬─name────────────────────┬─replicated─┬─active─┬─marks─┬─bytes─┬───modification_time─┬─────────remove_time─┬─refcount─┬───min_date─┬───max_date─┬─min_block_number─┬─max_block_number─┬─level─┬─database─┬─table────────┬─engine────┐
│ 197001    │ 19700101_19700101_2_2_0 │          0 │      1 │     1 │   628 │ 2017-06-07 10:39:27 │ 0000-00-00 00:00:00 │        2 │ 0000-00-00 │ 0000-00-00 │                2 │                2 │     0 │ default  │ test_table_1 │ MergeTree │
│ 198001    │ 19800101_19800101_4_4_0 │          0 │      1 │     1 │   628 │ 2017-06-07 10:39:45 │ 0000-00-00 00:00:00 │        2 │ 1980-01-01 │ 1980-01-01 │                4 │                4 │     0 │ default  │ test_table_1 │ MergeTree │
└───────────┴─────────────────────────┴────────────┴────────┴───────┴───────┴─────────────────────┴─────────────────────┴──────────┴────────────┴────────────┴──────────────────┴──────────────────┴───────┴──────────┴──────────────┴───────────┘
```
### ColumnSource
使用ColumnSource读取`default.test_table`中的`eventName`列:
``` scala
object Main{
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()
  def main(args: Array[String]): Unit = {
    val columnStream = ColumnSource[String]("/opt/clickhouse/data/default/test_table/19700101_19700101_2_2_0","eventName")
    columnStream.toSink(Sink.foreach[String](println)).run
  }
}
```
输出结果:
```
OnClick
```
### DataPartSource
使用DataPartSource读取`default.test_teble`中的`19800101_19800101_4_4_0`数据块:
``` scala
object Main{
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()
  def main(args:Array[String]): Unit = {
    val dataPartStream = DataPartSource("/opt/clickhouse/data/default/test_table/19700101_19700101_2_2_0")
    dataPartStream.toSink(Sink.foreach[Record](println)).run
  }
}
```
输出结果:
```
Record(List((1,eventDate,Thu Jan 01 08:00:00 CST 1970), (7,eventId,1), (8,eventName,OnClick), (3,count,3)))
```
### TableSource
使用TableSource读取`default.test_table`中的数据:
``` scala
object Main{
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()
  def main(args:Array[String]): Unit = {
    val tableStream =  TableSource("/opt/clickhouse/data/default/test_table")
    tableStream.toSink(Sink.foreach[Record](println)).run
  }
}
```
输出结果:
```
Record(List((1,eventDate,Thu Jan 01 08:00:00 CST 1970), (7,eventId,1), (8,eventName,OnClick), (3,count,3)))
Record(List((1,eventDate,Tue Jan 01 08:00:00 CST 1980), (7,eventId,1), (8,eventName,OnClick), (3,count,3)))
```
## ClickHouse的结构说明:
### DataBase
### Table
### Partition
### DataPart
### Column
### Block
### Row

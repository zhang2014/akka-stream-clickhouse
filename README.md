# akka-stream-clickhouse
休闲娱乐产物,用于研究学习Clickhouse,如有问题欢迎指正! 
项目主要实现将ClickHouse数据以表的单位进行读取分析写入的操作,最终实现效果如下:
```
def transfrom(record:Record):Record = ???
TableSource("${ClickhouseHome}/data/default/test_table_1").map(transfrom).to(TableSink("${ClickhouseHome}/data/default/test_table_2"))
```
TODO:
[√] ColumnSource
[√] DataPartSource
[√] TableSource
[ ] ColumnSink
[ ] DataPartSink
[ ] TableSink

## ClickHouse的结构说明:
### DataBase
### Table
### Partition
### DataPart
### Column
### Block
### Row

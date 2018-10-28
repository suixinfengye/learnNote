[Structured Streaming源码解析](https://user-gold-cdn.xitu.io/2018/3/29/162723a3b94fbab1)

Structured Streaming 在编程模型上暴露给用户的是，每次持续查询看做面对全量数据（而不仅仅是本次执行新收到的数据），所以每次执行的结果是针对全量数据进行计算的结果

# Structured Streaming 实现思路与实现概述
## StreamExecution：持续查询的运转引擎
StreamExecution 成员变量
1. sources: streaming data 的产生端（比如 kafka 等）
2. logicalPlan: DataFrame/Dataset 的一系列变换（即计算逻辑）
3. sink: 最终结果写出的接收端（比如 file system 等）
4. currentBatchId: 当前执行的 id
5. batchCommitLog: 已经成功处理过的批次有哪些
6. offsetLog, availableOffsets, committedOffsets: 当前执行需要处理的 source data 的 meta 信息
7. offsetSeqMetadata: 当前执行的 watermark 信息

## StreamExecution 的持续查询
![](https://user-gold-cdn.xitu.io/2018/3/29/162724cd189fb34e?w=878&h=368&f=png&s=204213)

## StreamExecution 的持续查询（增量）
Structured Streaming 在编程模型上暴露给用户的是，每次持续查询看做面对全量数据（而不仅仅是本次执行信收到的数据），每次持续查询看做面对全量数据，但在具体实现上转换为增量的持续查询。

但是在实际执行过程中，由于全量数据会越攒越多，那么每次对全量数据进行计算的代价和消耗会越来越大。

Structured Streaming 的做法是：
引入全局范围、高可用的 StateStore
转全量为增量，即在每次执行时：
    先从 StateStore 里 restore 出上次执行后的状态
    然后加入本执行的新数据，再进行计算
    如果有状态改变，将把改变的状态重新 save 到 StateStore 里
为了在 Dataset/DataFrame 框架里完成对 StateStore 的 restore 和 save 操作，引入两个新的物理计划节点 —— StateStoreRestoreExec 和 StateStoreSaveExec

## 故障恢复
存储 source offsets 的 offsetLog，和存储计算状态的 StateStore，是全局高可用的

# CoolplaySpark
## 1.1 Structured Streaming 实现思路与实现概述

RDD VS Dataset/DataFrame

RDD[Person] : Person 作为一个整体,一维、只有行概念的数据集

Dataset/DataFrame : 类似数据库表格,一个 Person，有 name:String, age:Int, height:Double 三列,二维行+列的数据集

Dataset/DataFrame 存储方式无区别：两者在内存中的存储方式是完全一样的、是按照二维行列（UnsafeRow）来存的

与静态的 structured data 不同，动态的 streaming data 的行列数据表格是一直**无限增长**的（因为 streaming data 在源源不断地产生）

**每次持续查询看做面对全量数据（而不仅仅是本次执行信收到的数据），所以每次执行的结果是针对全量数据进行计算的结果**

StreamExecution 重要成员变量
1. sources: streaming data 的产生端（比如 kafka 等）
2. logicalPlan: DataFrame/Dataset 的一系列变换（即计算逻辑）
3. sink: 最终结果写出的接收端（比如 file system 等）
4. currentBatchId: 当前执行的 id
5. batchCommitLog: 已经成功处理过的批次有哪些
6. offsetLog, availableOffsets, committedOffsets: 当前执行需要处理的 source data 的 meta 信息
7. offsetSeqMetadata: 当前执行的 watermark 信息等

![StreamExecution 的持续查询](https://user-gold-cdn.xitu.io/2018/6/4/163c8c8b8e931c9c?w=878&h=368&f=png&s=204213)
(紫色代表高可用)
6 个关键步骤：

1. StreamExecution 通过 Source.getOffset() 获取最新的 offsets，即最新的数据进度；
> Kafka (KafkaSource) 的具体 getOffset() 实现 ，会通过在 driver 端的一个长时运行的 consumer 从 kafka brokers 处获取到各个 topic 最新的 offsets（注意这里不存在 driver 或 consumer 直接连 zookeeper）

2. StreamExecution 将 offsets 等写入到 offsetLog 里
    
    这里的 offsetLog 是一个持久化的 WAL (Write-Ahead-Log)，是将来可用作故障恢复用
3. StreamExecution 构造本次执行的 LogicalPlan

    (3a) 将预先定义好的逻辑（即 StreamExecution 里的 logicalPlan 成员变量）制作一个副本出来

    (3b) 给定刚刚取到的 offsets，通过 Source.getBatch(start: Option[Offset], end: Offset), 左开右闭(start:上一个执行批次里提供的最新 offset, end:这个批次里提供的最新 offset],获取本执行新收到的数据的 Dataset/DataFrame 表示(只包含数据的描述信息，并没有发生实际的取数据操作）)，并替换到 (3a) 中的副本里
    
    > 进行 event time 的字段 <= watermark 的过滤:将 driver 端的 watermark 最新值（即已经写入到 offsetLog 里的值）作为过滤条件，加入到整个执行的 logicalPlan 中
    
    经过 (3a), (3b) 两步，构造完成的 LogicalPlan 就是针对本执行新收到的数据的 Dataset/DataFrame 变换（即整个处理逻辑）了
4. 触发对本次执行的 LogicalPlan 的优化，得到 IncrementalExecution

    逻辑计划的优化：通过 Catalyst 优化器完成
    
    物理计划的生成与选择：结果是可以直接用于执行的 RDD DAG
    
    逻辑计划、优化的逻辑计划、物理计划、及最后结果 RDD DAG，合并起来就是 IncrementalExecution
5. 将表示计算结果的 Dataset/DataFrame (包含 IncrementalExecution) 交给 Sink，即调用 Sink.addBatch(batchId: Long, data: DataFrame)

    这时才会由 Sink 触发发生实际的取数据操作，以及计算过程
    
6. 计算完成后的 commit

    (6a) 通过 Source.commit() 告知 Source 数据已经完整处理结束；Source 可按需完成数据的 garbage-collection
    
    (6b) 将本次执行的批次 id 写入到 batchCommitLog 里

在实际执行过程中，由于全量数据会越攒越多，那么每次对全量数据进行计算的代价和消耗会越来越大

Structured Streaming 的做法是：

引入全局范围、高可用的 StateStore

转全量为增量，即在每次执行时：

    先从 StateStore 里 restore 出上次执行后的状态
    
    然后加入本执行的新数据，再进行计算
    
    如果有状态改变，将把改变的状态重新 save 到 StateStore 里
    
为了在 Dataset/DataFrame 框架里完成对 StateStore 的 restore 和 save 操作，引入两个新的物理计划节点 —— StateStoreRestoreExec 和 StateStoreSaveExec

所以 Structured Streaming 在编程模型上暴露给用户的是，**每次持续查询看做面对全量数据，但在具体实现上转换为增量的持续查询**。

**故障恢复**

如果在某个执行过程中发生 driver 故障，那么重新起来的 StreamExecution：

读取 WAL offsetlog 恢复出最新的 offsets 等；相当于取代正常流程里的 (1)(2) 步

读取 batchCommitLog 决定是否需要重做最近一个批次

如果需要，那么重做 (3a), (3b), (4), (5), (6a), (6b) 步

这里第 (5) 步需要分两种情况讨论

(i) 如果上次执行在 (5) 结束前即失效，那么本次执行里 sink 应该完整写出计算结果

(ii) 如果上次执行在 (5) 结束后才失效，那么本次执行里 sink 可以重新写出计算结果（覆盖上次结果），也可以跳过写出计算结果（因为上次执行已经完整写出过计算结果了）

这样即可保证每次执行的计算结果，在 sink 这个层面，是 不重不丢 的 —— 即使中间发生过 1 次或以上的失效和恢复


Source，需能 根据 offsets 重放数据 
Sources | 是否可重放 | 原生内置支持 | 注解
---	|	---	|	---	|	---
HDFS-compatible file system | checked | 已支持 | 包括但不限于 text, json, csv, parquet, orc, ...
Kafka | checked | 已支持 | Kafka 0.10.0+
RateStream | checked | 已支持 | 以一定速率产生数据
RDBMS | checked | (待支持) | 预计后续很快会支持
Socket | negative | 已支持 | 主要用途是在技术会议/讲座上做 demo
Receiver-based | negative | 不会支持 | 就让这些前浪被拍在沙滩上吧

Sink，需能 幂等式写入数据
Sinks | 是否幂等写入 | 原生内置支持 | 注解
--- | --- | --- | ---
HDFS-compatible file system | checked | 已支持 | 包括但不限于 text, json, csv, parquet, orc, ...
ForeachSink (自定操作幂等) | checked | 已支持 | 可定制度非常高的 sink
RDBMS | checked | (待支持) | 预计后续很快会支持
Kafka | negative | 已支持 | Kafka 目前不支持幂等写入，所以可能会有重复写入（但推荐接着 Kafka 使用 streaming de-duplication 来去重）
ForeachSink (自定操作不幂等) | negative | 已支持 | 不推荐使用不幂等的自定操作
Console | negative | 已支持 | 主要用途是在技术会议/讲座上做 demo

**offset tarcking in WAL + state management + fault-tolerance source and sinks  = end-to-end exactly-once guarantees** 

## Structured Streaming 之 Sink 解析
- Sink 的具体实现: HDFS-API compatible FS
```scala
writeStream
  .format("parquet")      // parquet, csv, json, text, orc ...
  .option("checkpointLocation", "path/to/checkpoint/dir")
  .option("path", "path/to/destination/dir")
```
是由FileStreamSink.write 写入数据的

- 具体实现: Foreach
```
writeStream
  /* 假设进来的每条数据是 String 类型的 */
  .foreach(new ForeachWriter[String] {
    /* 每个 partition 即每个 task 会在开始时调用此 open() 方法,要写成幂等的,保证 end-to-end exactly-once guarantees  */
    /* 注意对于同一个 partitionId/version，此方法可能被先后调用多次，如 task 失效重做时 */
    /* 注意对于同一个 partitionId/version，此方法也可能被同时调用，如推测执行时 */
    override def open(partitionId: Long, version: Long): Boolean = {
      println(s"open($partitionId, $version)")
      true
    }
    /* 此 partition 内即每个 task 内的每条数据，此方法都被调用 */
    override def process(value: String): Unit = println(s"process $value")
    /* 正常结束或异常结束时，此方法被调用。但一些异常情况时，此方法不一定被调用。 */
    override def close(errorOrNull: Throwable): Unit = println(s"close($errorOrNull)")
  })
```
数据通过ForeachSink写入
```scala
  // 来自：class ForeachSink extends Sink with Serializable
  // 版本：Spark 2.1.0
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val encoder = encoderFor[T].resolveAndBind(
      data.logicalPlan.output,
      data.sparkSession.sessionState.analyzer)
    /* 是 rdd 的 foreachPartition，即是 task 级别 */
    data.queryExecution.toRdd.foreachPartition { iter =>
      /* partition/task 级别的 open */
      if (writer.open(TaskContext.getPartitionId(), batchId)) {
        try {
          while (iter.hasNext) {
            /* 对每条数据调用 process() 方法 */
            writer.process(encoder.fromRow(iter.next()))
          }
        } catch {
          case e: Throwable =>
            /* 异常时调用 close() 方法 */
            writer.close(e)
            throw e
        }
        /* 正常写完调用 close() 方法 */
        writer.close(null)
      } else {
        /* 不写数据、直接调用 close() 方法 */
        writer.close(null)
      }
    }
  }
```

- 具体实现: Kafka
```scala
writeStream
  .format("kafka")
  .option("checkpointLocation", ...)
  .outputMode(...)
  .option("kafka.bootstrap.servers", ...) // 写出到哪个集群
  .option("topic", ...) // 写出到哪个 topic
```
```scala
//KafkaSink.addBatch()
//    KafkaWriter.write()
//        KafkaWriteTask.execute()
def execute(iterator: Iterator[InternalRow]): Unit = {
    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)
    while (iterator.hasNext && failedWrite == null) {
     ....
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic.toString, key, value)
      val callback = new Callback() {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (failedWrite == null && e != null) {
            failedWrite = e
          }
        }
      }
      producer.send(record, callback)
    }
  }
```
由于 Spark 本身会失败重做 —— 包括单个 task 的失败重做、stage 的失败重做、整个拓扑的失败重做等 —— 那么同一条数据可能被写入到 kafka 一次以上

## Structured Streaming 之状态存储解析
类似 count() 的聚合式持续查询,本批次的执行只需依赖上一个批次的结果，而不需要依赖之前所有批次的结果。这也即增量式持续查询，能够将每个批次的执行时间稳定下来，避免越后面的批次执行时间越长的情形

### StateStore 模块的总体思路
- 分布式实现

跑在现有 Spark 的 driver-executors 架构上

driver 端是轻量级的 coordinator，只做协调工作

executor 端负责状态的实际分片的读写
![](https://user-gold-cdn.xitu.io/2018/6/4/163ca0bea9fbbbbd?w=655&h=364&f=png&s=68571)
每个executor端中的状态分片是保存了部分StateStore数据还是全量数据?看来是以operator +partition储存,不是存储一整个StateStore数据

- 状态分片
分片以 operatorId+partitionId 为切分依据,同一个 operator 同一个 partition 的状态也是随着时间不断更新、产生新版本的数据,所以有operator + partition + version,表明某个操作下partition载某个批次的值,operator就是操作

读入分片:根据 operator + partition + version， 从 HDFS 读入数据，并缓存在内存里

写出分片:将当前版本的修改一次性写出到 HDFS 一个修改的流水 log，流水 log 写完即标志本批次的状态修改完成

 operator, partiton, version 
![](https://user-gold-cdn.xitu.io/2018/6/4/163ca0a4d3d04da4?w=450&h=310&f=png&s=41133)

### StateStore：(a)迁移、(b)更新和查询、(c)维护、(d)故障恢复
本机或者远程读取需要用到的operator+partition+version 数据即可

StateStore 内部使用示例
```scala
 // 在最开始，获取正确的状态分片(按需重用已有分片或读入新的分片)
  val store = StateStore.get(StateStoreId(checkpointLocation, operatorId, partitionId), ..., version, ...)

  // 开始进行一些更改
  store.put(...)
  store.remove(...)
    
  // 更改完成，批量提交缓存在内存里的更改到 HDFS
  store.commit()
    
  // 查看当前状态分片的所有 key-value 
  store.iterator()
  // 查看刚刚更新了的 key-value
  store.updates()
```
StateStore 本身也带了 maintainess 即维护模块，会周期性的在后台将过去的状态和最近若干版本的流水 log 进行合并，并把合并后的结果重新写回到 HDFS：old_snapshot + delta_a + delta_b + … => lastest_snapshot。

从 HDFS 读入最近可见的状态时，如果有最新的 snapshot，也就用最新的 snapshot，如果没有，就读入稍旧一点的 snapshot 和新的 deltas，先做一下最新状态的合并。

如果某个状态分片在更新过程中失败了，那么还没有写出的更新会不可见。
恢复时也是从 HDFS 读入最近可见的状态，并配合 StreamExecution 的执行批次重做

# Structured Streaming 之 Event Time 解析
OutputModes

Complete 的输出是和 State 是完全一致的

Append 一旦输出了某条 key，未来就不会再输出同一个 key。延迟数据用了watermark 

Update 只有本执行批次 State 中被更新了的条目会被输出

# Structured Streaming 之 Watermark 解析
Watermark 机制用于Append 模式或 Update 模式,但不需状态做跨执行批次的聚合时，则不需要启用 watermark 机制。
```scala
val words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

// Group the data by window and word and compute the count of each group
val windowedCounts = words
    .withWatermark("timestamp", "10 minutes")  // 注意这里的 watermark 设置！
    .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"),
        $"word")
    .count()
```
![](https://user-gold-cdn.xitu.io/2018/6/4/163ca3305e41db0d)
在 12:30 批次结束时，即知道 event time 12:10 以前的数据不再收到了，因而 window 12:00-12:10 的结果也不会再被更新，即可以安全地输出结果 12:00-12:10|cat|2

在结果 12:00-12:10|cat|2 输出以后，State 中也不再保存 window 12:00-12:10 的相关信息 —— 也即 State Store 中的此条状态得到了清理

每次 StreamExecution 的每次增量执行（即 IncrementalExecution）开始**后**，首先会在 driver 端持久化相关的 source offsets 到 offsetLog 中。实际在这个过程中，也将系统当前的 watermark 等值保存了进去,在故障恢复时，可以从 offsetLog 中恢复出来的 watermark 值

在每次 StreamExecution 的每次增量执行（即 IncrementalExecution）开始时，将 driver 端的 watermark 最新值（即已经写入到 offsetLog 里的值）作为过滤条件，加入到整个执行的 logicalPlan 中。

> watermark写入时机有点问题

在单次增量执行过程中，具体的是在做 (b) Watermark 用作过滤条件 的过滤过程中，watermark 维持不变。

直到在单次增量执行结束时，根据收集到的 eventTimeStats，才更新一个 watermark。更新后的 watermark 会被保存和故障时恢复，这个过程是我们在 (a) Watermark 的保存和恢复 中解析的。

在对 event time 做 window() + groupBy().aggregation() 即利用状态做跨执行批次的聚合，并且输出模式为 Append 模式或 Update 模式时，才需要 watermark，其它时候不需要；

watermark 的本质是要帮助 StateStore 清理状态、不至于使 StateStore 无限增长；同时，维护 Append 正确的语义（即判断在何时某条结果不再改变、从而将其输出）；




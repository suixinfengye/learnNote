# Spark Streaming 实现思路与模块概述
Spark Streaming 的整体模块划分
![Spark Streaming 的整体模块划分](https://user-gold-cdn.xitu.io/2018/5/10/1634a634d959d8be?w=2471&h=1083&f=png&s=128850)
![](https://user-gold-cdn.xitu.io/2018/5/10/1634a7664559c225?w=1058&h=742&f=png&s=43109)
![](https://user-gold-cdn.xitu.io/2018/5/10/1634a76ae64c763b?w=1029&h=1260&f=png&s=81001)
![](https://user-gold-cdn.xitu.io/2018/5/10/1634a76d1cb49a92?w=853&h=1258&f=png&s=80240)

```scala
import org.apache.spark._
import org.apache.spark.streaming._

// 首先配置一下本 quick example 将跑在本机，app name 是 NetworkWordCount
val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
// batchDuration 设置为 1 秒，然后创建一个 streaming 入口
val ssc = new StreamingContext(conf, Seconds(1))

// ssc.socketTextStream() 将创建一个 SocketInputDStream；这个 InputDStream 的 SocketReceiver 将监听本机 9999 端口
val lines = ssc.socketTextStream("localhost", 9999)

val words = lines.flatMap(_.split(" "))      // DStream transformation
val pairs = words.map(word => (word, 1))     // DStream transformation
val wordCounts = pairs.reduceByKey(_ + _)    // DStream transformation
wordCounts.print()                           // DStream output
// 上面 4 行利用 DStream transformation 构造出了 lines -> words -> pairs -> wordCounts -> .print() 这样一个 DStreamGraph
// 但注意，到目前是定义好了产生数据的 SocketReceiver，以及一个 DStreamGraph，这些都是静态的

// 下面这行 start() 将在幕后启动 JobScheduler, 进而启动 JobGenerator 和 ReceiverTracker
// ssc.start()
//    -> JobScheduler.start()
//        -> JobGenerator.start();    开始不断生成一个一个 batch
//        -> ReceiverTracker.start(); 将 Receiver 分发到多个 executor 上去。在每个 executor 上由 ReceiverSupervisor 来分别启动一个 Receiver 接收数据
//              ->launchReceivers()
ssc.start()

// 然后用户 code 主线程就 block 在下面这行代码了
// block 的后果就是，后台的 JobScheduler 线程周而复始的产生一个一个 batch 而不停息
// 也就是在这里，我们前面静态定义的 DStreamGraph 的 print()，才一次一次被在 RDD 实例上调用，一次一次打印出当前 batch 的结果
ssc.awaitTermination()
```

# DAG 静态定义
## DStream, DStreamGraph 详解
```scala
// ssc.socketTextStream() 将创建一个 SocketInputDStream；这个 InputDStream 的 SocketReceiver 将监听本机 9999 端口
val lines = ssc.socketTextStream("localhost", 9999)

val words = lines.flatMap(_.split(" "))      // DStream transformation
val pairs = words.map(word => (word, 1))     // DStream transformation
val wordCounts = pairs.reduceByKey(_ + _)    // DStream transformation
wordCounts.print()       
```
具体实现
```scala
val lines = new SocketInputDStream("localhost", 9999)   // 类型是 SocketInputDStream

val words = new FlatMappedDStream(lines, _.split(" "))  // 类型是 FlatMappedDStream
val pairs = new MappedDStream(words, word => (word, 1)) // 类型是 MappedDStream
val wordCounts = new ShuffledDStream(pairs, _ + _)      // 类型是 ShuffledDStream
new ForeachDStream(wordCounts, cnt => cnt.print())      // 类型是 ForeachDStream
```
transformation:基本上 1 种 transformation 将对应产生一个新的 DStream 子类实例

output：将只产生一种 ForEachDStream 子类实例，用一个函数 func 来记录需要做的操作,如对于 `print()` 就是：`func = cnt => cnt.print()`

# Dependency, DStreamGraph 解析
![](https://user-gold-cdn.xitu.io/2018/5/10/1634a93b99fb9580?w=680&h=260&f=png&s=62395)
(1) DStream 逻辑上通过 transformation 来形成 DAG，但在物理上却是通过与 transformation 反向的依赖（dependency）来构成表示的

(2) 当某个节点调用了 output 操作时，就产生一个新的 ForEachDStream ，这个新的 ForEachDStream 记录了具体的 output 操作是什么

(3) 在每个 batch 动态生成 RDD 实例时，就对 (2) 中新生成的 DStream 进行 BFS 遍历

(4) Spark Streaming 记录整个 DStream DAG 的方式，就是通过一个 DStreamGraph 实例记录了到所有的 output stream 节点的引用

(5) DStreamGraph 实例同时也记录了到所有 input stream 节点的引用

## DStream 生成 RDD 实例详解
DStream 内部用一个类型是 HashMap 的变量 generatedRDD 来记录已经生成过的 RDD：
```scala
//这个 Time 是与用户指定的 batchDuration 对齐了的时间
//如每 15s 生成一个 batch 的话，那么这里的 key 的时间就是 08h:00m:00s，08h:00m:15s 这种，
//所以其实也就代表是第几个 batch。generatedRDD 的 value 就是 RDD 的实例
private[streaming] var generatedRDDs = new HashMap[Time, RDD[T]] ()
```
**每一个不同的 DStream 实例，都有一个自己的 generatedRDD**
```
//DStream 对这个 HashMap 的存取主要是通过 getOrCompute(time: Time) 方法，
//实现也很简单，就是一个 —— 查表，如果有就直接返回，如果没有就生成了放入表、再返回
DStream.getOrCompute(time: Time)
   generatedRDDs.get(time).orElse {
        if isTimeValid(time) :  compute(time)   //用于生成 RDD 实例,由子类实现
        foreach newRDD : 
            newRDD.persist(storageLevel)    //storageLevel != StorageLevel.NONE
            newRDD.checkpoint() //符合checkpoint区间时间
            generatedRDDs.put(time, newRDD)
        }
```

```
//InputDStream 的 compute(time) 实现(没有上游依赖的 DStream)
FileInputDStream.compute(validTime: Time)
    //找到 validTime 以后产生的新 file 的数据
    val newFiles = findNewFiles(validTime.milliseconds)
    //生成单个 RDD 实例 rdds
    val rdds = Some(filesToRDD(newFiles))
         //对每个新 file，都将其作为参数调用 sc.newAPIHadoopFile(file)，生成一个 RDD 实例
        val fileRDDs = newFiles.foreach {context.sparkContext.newAPIHadoopFile() }
        // 将每个 file 对应的 RDD 进行 union，返回一个 union 后的 UnionRDD
        new UnionRDD(context.sparkContext, fileRDDs)
        inputInfoTracker.reportInfo(validTime, StreamInputInfo)
```

```
//有上游依赖的RDD,先获取上游依赖的 DStream 产生的 RDD 实例
MappedDStream.compute(time)
    //获取 parent DStream 在本 batch 里对应的 RDD 实例
    //在这个 parent RDD 实例上，以 mapFunc 为参数调用 .map(mapFunc) 方法，将得到的新 RDD 实例返回
    parent.getOrCompute(validTime).map(_.map[U](mapFunc))

FilteredDStream.compute(time)
     parent.getOrCompute(validTime).map(_.filter(filterFunc))
```
```
//ForEachDStream 的 compute(time) 实现:DStream output 操作
ForEachDStream.compute(Time): Option[RDD[Unit]] = None
ForEachDStream.generateJob()
    //获取 parent DStream 在本 batch 里对应的 RDD 实例
    //parent.getOrCompute()是个递归方法,会一直递归调用到最初DStream 
    parent.getOrCompute(time)
        createRDDWithLocalProperties()
        //foreachFunc是本次 output 的具体函数
        new Job(time, foreachFunc(rdd, time))
```

## DStreamGraph 生成 RDD DAG 实例
```
ssc.start()                              // 【用户 code：StreamingContext.start()】
    -> scheduler.start()                 // 【JobScheduler.start()】
                 -> jobGenerator.start() // 【JobGenerator.start()】
```
```
JobGenerator.scala
    timer = new RecurringTimer(ssc.graph.batchDuration.milliseconds,eventLoop.post(GenerateJobs(new Time(longTime))))
=>  start()
        new CheckpointWriter()
        eventLoop = new EventLoop[JobGeneratorEvent].start()
        if ssc.isCheckpointPresent
            restart() //如果不是第一次启动，就需要从 checkpoint 恢复
        else startFirstTime()  //第一次启动
            new Time(timer.getStartTime()).start()
            graph.start()
        
    //定时周期
=>  receive : generateJobs()
        //要求 ReceiverTracker 将目前已收到的数据进行一次 allocate，即将上次 batch 切分后的数据切分到到本次新的 batch 里
        //对 batch 的源头数据 meta 信息进行了 batch 的分配
        jobScheduler.receiverTracker.allocateBlocksToBatch(time)
        //要求 DStreamGraph 复制出一套新的 RDD DAG 的实例
        graph.generateJobs(time)
        //按照 batch 时间来向 ReceiverTracker 查询得到划分到本 batch 的块数据 meta 信息
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        //拿到Seq[Job]后包装成一个 JobSet,提交异步执行
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
            //每个 job 都在 jobExecutor 线程池中、用 JobHandler 来处理
            jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
        //对整个系统的当前运行状态做一个 checkpoint
        eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
```
```java
//返回的是一组job,一个batch可能有多个action操作
DStreamGraph.generateJobs(time: Time): Seq[Job]
    //有几个 output 操作，就调用几次 ForEachDStream.generatorJob(time)，就产生出几个 Job
    forreach outputStream : generateJob(time)
        //每个 ForEachDStream 都通过 generateJob(time) 方法贡献了一个 Job
        ForEachDStream.generateJob(time)//见上文
```
Spark Streaming 的 Job
```scala
//Job.scala
//类似runnable,func() 定义和 func() 调用分离了,如线程的run
def run() {
    _result = Try(func())
  }
```

# Job 动态生成
## JobScheduler, Job, JobSet 详解
```
StreamingContext.start()
    ThreadUtils.runInNewThread("streaming-start"){JobScheduler.start()}
 
 JobScheduler.start()
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler").start()
    new ReceiverTracker(ssc).start()
    jobGenerator.start()
```
```
JobHandler.run()
    _eventLoop.post(JobStarted(job, clock.getTimeMillis()))
    job.run() //真正运行job
        _result = Try(func())//调用分离
    _eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
```

```scala
//JobScheduler.scala
//jobExecutor 的线程池能够并行执行的 Job 数
  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
  //Job 将在这个线程池的线程里，被实际执行 func
  private val jobExecutor =
    ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")
```

## JobGenerator 详解
```
// 来自 JobGenerator.start()

def start(): Unit = synchronized {
  ...
  eventLoop.start()                      // 【启动 RPC 处理线程】
  if (ssc.isCheckpointPresent) {
    restart()                            // 【如果不是第一次启动，就需要从 checkpoint 恢复】
  } else {
    startFirstTime()                     // 【第一次启动，就 startFirstTime()】
  }
}
```

# Receiver 分发详解
```
ssc.start()
=>  JobGenerator.start()    //开始不断生成一个一个 batch
    ReceiverTracker.start() //将 Receiver 分发到多个 executor 上去。在每个 executor 上由 ReceiverSupervisor 来分别启动一个 Receiver 接收数据
=>      ReceiverTracker.lunchReceivers()
            foreach InputStreams : getReceiver() //如 new KafkaReceiver 返回
            endpoint.send(StartAllReceivers(receivers))    

ReceiverTracker.receive //executor端
=>  case StartAllReceivers(receivers) 
        rceiverSchedulingPolicy.scheduleReceivers(receivers, getExecutors) //计算每个 Receiver 的目的地 executor 列表
        foreach receiver :  startReceiver(receiver, executors)
            new ReceiverSupervisorImpl(receiver).start()
=>              foreach  BlockGenerators :  _.start() //攒数据成块储存
                receiverTrackerEndpoint.askSync(RegisterReceiver)
                receiver.onStart() //启动 Receiver 的数据接收线程
```

### 可插拔的 ReceiverSchedulingPolicy
ReceiverSchedulingPolicy 的主要目的，是在 Spark Streaming 层面添加对 Receiver 的分发目的地的计算，ReceiverSchedulingPolicy 能计算出更好的分发策略。分发策略为round-robin循环式

ReceiverSchedulingPolicy.scheduleReceivers(receivers, executors) 来计算每个 Receiver 的目的地 executor 列表

重启某个 Receiver : ReceiverSchedulingPolicy.rescheduleReceiver(receiver, ...) 来重新计算这个 Receiver 的目的地 executor 列表

### 每个 Receiver 分发有单独的 Job 负责
每个 Receiver 都分配单独的只有 **1 个 Task 的 Job** 来尝试分发(每个 Receiver 都有专门的 Job 来保证分发)

现在的 Receiver 重试不是在 Task 级别，而是在 Job 级别；并且 Receiver 失效后并不会导致前一次 Job 失败，而是前一次 Job 成功、并新起一个 Job 再次进行分发。这样一来，不管 Spark Streaming 运行多长时间，Receiver 总是保持活性的，不会随着 executor 的丢失而导致 Receiver 死去,Receiver 的失效重启就不受 spark.task.maxFailures(默认=4) 次的限制了

如果一次 Receiver 如果没有抵达预先计算好的 executor，就有机会再次进行分发

# Receiver, ReceiverSupervisor, BlockGenerator, ReceivedBlockHandler 详解

![数据接收存储](https://user-gold-cdn.xitu.io/2018/5/31/163b423e3d9db71d?w=600&h=219&f=png&s=37034)


## Receiver 详解
```
// 来自 Receiver

abstract class Receiver[T](val storageLevel: StorageLevel) extends Serializable {
  // 需要子类实现,线程异步
  def onStart()
  def onStop()
  
  // 基类实现，供子类调用
  def store(dataItem: T) {...}                  // 【存储单条小数据】
  def store(dataBuffer: ArrayBuffer[T]) {...}   // 【存储数组形式的块数据】
  def store(dataIterator: Iterator[T]) {...}    // 【存储 iterator 形式的块数据】
  def store(bytes: ByteBuffer) {...}            // 【存储 ByteBuffer 形式的块数据】
  
  ...
}
```
store() 的实现都是直接将数据转给 ReceiverSupervisor，由 ReceiverSupervisor 来具体负责存储

## ReceiverSupervisor 详解

executor 端会先 new 一个 ReceiverSupervisorImpl，然后 ReceiverSupervisorImpl.start()。这里 .start() 很重要的工作就是调用 Receiver.onStart()，来启动 Receiver 的数据接收线程

ReceiverSurpervisorImpl 有 4 种签名的 push() 方法，被 Receiver 的 4 种 store() 一一调用
```
// 来自 ReceiverSupervisorImpl
//非单条数据

def pushArrayBuffer(arrayBuffer: ArrayBuffer[_], ...) {
  pushAndReportBlock(ArrayBufferBlock(...), ...)
}
def pushIterator(iterator: Iterator[_], ...) {
  pushAndReportBlock(IteratorBlock(...), ...)
}
def pushBytes(bytes: ByteBuffer, ...){
  pushAndReportBlock(ByteBufferBlock(...), ...)
}
def pushAndReportBlock(receivedBlock: ReceivedBlock, ...) {
    //ReceivedBlock 交给 ReceivedBlockHandler 来存储
    receivedBlockHandler.storeBlock(blockId, receivedBlock)
    //将已存储好的 ReceivedBlock 的块数据 meta 信息报告给 ReceiverTracker
    trackerEndpoint.askSync[Boolean](AddBlock(blockInfo))
}
```

单条的情况，ReceiverSupervisorImpl 要在 BlockGenerator 的协助下，将多个单条的数据积攒为一个块数据，然后重新调用 push 交给 ReceiverSurpervisorImpl 来处理这个块数据
```scala
//单条数据
  def pushSingle(data: Any) {
    //currentBuffer = new ArrayBuffer[Any],加到这个数组中
    defaultBlockGenerator.addData(data)
  }
```
## ReceivedBlockHandler
在 executor 端负责对接收到的块数据进行具体的存储和清理

ReceivedBlockHandler 有两个具体的存储策略的实现：

(a) BlockManagerBasedBlockHandler，是直接存到 executor BlockManager的内存或硬盘
(b) WriteAheadLogBasedBlockHandler，是先写 WAL，再存储到 executor 的内存或硬盘
```scala
// 来自 WriteAheadLogBasedBlockHandler

def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {
  ...
  // 【生成向 BlockManager 存储数据的 future】
  val storeInBlockManagerFuture = Future {
    val putResult =
      blockManager.putBytes(blockId, serializedBlock, effectiveStorageLevel, tellMaster = true)
    if (!putResult.map { _._1 }.contains(blockId)) {
      throw new SparkException(
        s"Could not store $blockId to block manager with storage level $storageLevel")
    }
  }

  // 【生成向 WAL 存储数据的 future】
  val storeInWriteAheadLogFuture = Future {
    writeAheadLog.write(serializedBlock, clock.getTimeMillis())
  }

  // 【开始执行两个 future、等待两个 future 都结束】
  val combinedFuture = storeInBlockManagerFuture.zip(storeInWriteAheadLogFuture).map(_._2)
  val walRecordHandle = Await.result(combinedFuture, blockStoreTimeout)
  
  // 【返回存储结果，用于后续的块数据 meta 上报】
  WriteAheadLogBasedStoreResult(blockId, numRecords, walRecordHandle)
}
```

# BlockGenerator 详解
BlockGenerator 在内部主要是维护一个临时的变长数组 currentBuffer，每收到一条 ReceiverSupervisorImpl 转发来的数据就加入到这个 currentBuffer 数组中
```scala
//BlockGenerator.scala
private var currentBuffer = new ArrayBuffer[Any]
 def addData(data: Any): Unit = {
    waitToPush()    //控制添加数据速率
    currentBuffer += data
  }
```
在加入 currentBuffer 数组时会先由 rateLimiter 检查一下速率，是否加入的频率已经太高。如果太高的话，就需要 block 住，等到下一秒再开始添加。这里的最高频率是由 spark.streaming.receiver.maxRate (default = Long.MaxValue) 控制的，是单个 Receiver 每秒钟允许添加的条数。控制了这个速率，就控制了整个 Spark Streaming 系统每个 batch 需要处理的最大数据量。

然后会维护一个定时器，每隔 blockInterval 的时间就生成一个新的空变长数组替换老的数组作为新的 currentBuffer ，并把老的数组加入到一个自己的一个 blocksForPushing 的队列里,有另外的一个线程专门从这个队列里取出来已经包装好的块数据,将块数据交回给 ReceiverSupervisorImpl。

# ReceiverTraker, ReceivedBlockTracker 详解

## ReceiverTracker 详解
(1) ReceiverTracker 分发和监控 Receiver
ReceiverTracker 负责 Receiver 在各个 executor 上的分发
包括 Receiver 的失败重启

(2) ReceiverTracker 作为 RpcEndpoint
ReceiverTracker 作为 Receiver 的管理者，是各个 Receiver 上报信息的入口
也是 driver 下达管理命令到 Receiver 的出口

(3) ReceiverTracker 管理已上报的块数据 meta 信息
一方面 Receiver 将通过 AddBlock 消息上报 meta 信息给 ReceiverTracker，另一方面 JobGenerator 将在每个 batch 开始时要求 ReceiverTracker 将已上报的块信息进行 batch 划分.
ReceivedBlockTracker，专门负责已上报的块数据 meta 信息管理

**整体来看，ReceiverTracker 就是 Receiver 相关信息的中枢**。

ReceiverTracker 支持的消息有 10 种
- | 消息 | 解释
--- | --- | ----
ReceiverTracker只接收、不回复 | StartAllReceivers 消息 | 在 ReceiverTracker 刚启动时，发给自己这个消息，触发具体的 schedulingPolicy 计算，和后续分发
- | RestartReceiver 消息 | 当初始分发的 executor 不对，或者 Receiver 失效等情况出现，发给自己这个消息，触发 Receiver 重新分发
- |CleanupOldBlocks 消息 | 当块数据已完成计算不再需要时，发给自己这个消息，将给所有的 Receiver 转发此 CleanupOldBlocks 消息
- |UpdateReceiverRateLimit 消息 | ReceiverTracker 动态计算出某个 Receiver 新的 rate limit，将给具体的 Receiver 发送 UpdateRateLimit 消息
- |ReportError 消息 | 是由 Receiver 上报上来的，将触发 reportError() 方法向 listenerBus 扩散此 error 消息
ReceiverTracker接收并回复 | RegisterReceiver 消息 | 由 Receiver 在试图启动的过程中发来，将回复允许启动，或不允许启动
- | AddBlock 消息 | 具体的块数据 meta 上报消息，由 Receiver 发来，将返回成功或失败
- | DeregisterReceiver 消息 | 由 Receiver 发来，处理后，无论如何都返回 true
- | AllReceiverIds 消息 | 在 ReceiverTracker stop() 的过程中，查询是否还有活跃的 Receiver
- | StopAllReceivers 消息 | 在 ReceiverTracker stop() 的过程刚开始时，要求 stop 所有的 Receiver；将向所有的 Receiver 发送 stop 信息

## ReceivedBlockTracker 详解
ReceivedBlockTracker 专门负责已上报的块数据 meta 信息管理,但 ReceivedBlockTracker 本身不负责对外交互，一切都是通过 ReceiverTracker 来转发
```scala
  //维护了上报上来的、但尚未分配入 batch 的 Block 块数据的 meta,为每个 Receiver 单独维护一个 queue
  private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]
  //为每个 Receiver 单独维护一个 queue,按照 batch 进行一级索引、再按照 receiverId 进行二级索引的 queue，所以是一个 HashMap: time → HashMap
  private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]
  //记录了最近一个分配完成的 batch 是哪个
  private var lastAllocatedBatchTime: Time = null
```

# Executor 端长时容错详解
在 executor 端，ReceiverSupervisor 和 Receiver 失效后直接重启就 OK 了，关键是保障收到的块数据的安全

对源头块数据的保障
(1) 热备
(2) 冷备
(3) 重放
(4) 忽略

## 热备
在存储块数据时，将其存储到本 executor、并同时 replicate 到另外一个 executor 上去。这样在一个 replica 失效后，可以立刻无感知切换到另一份 replica 进行计算

本质上还是依靠 BlockManager 进行热备

## 冷备
除了存储到本 executor，还会把块数据作为 log 写出到 WriteAheadLog 里作为冷备。这样当 executor 失效时，就由另外的 executor 去读 WAL，再重做 log 来恢复块数据

恢复时可能需要一段 recover time

### WriteAheadLog 框架

WriteAheadLog 的特点是顺序写入，所以在做数据备份时效率较高，但在需要恢复数据时又需要顺序读取，所以需要一定 recovery time

Spark Streaming 没有后续对块数据的追加、修改、删除操作,WAL 里只会有一条此块数据的 log entry,在恢复时只要 seek 到这条 log entry 并读取就可以了，而不需要顺序读取整个 WAL

基于文件WAL: log 写到一个文件里,然后每隔一段时间就关闭已有文件，产生一些新文件继续写

## 重放
如果上游支持重放，比如 Apache Kafka，那么就可以选择不用热备或者冷备来另外存储数据了，而是在失效时换一个 executor 进行数据重放即可。

Spark Streaming 从 Kafka 读取方式有两种：

- 基于 Receiver 的
这种是将 Kafka Consumer 的偏移管理交给 Kafka —— 将存在 ZooKeeper 里，失效后由 Kafka 去基于 offset 进行重放
 
这样可能的问题是，Kafka 将同一个 offset 的数据，重放给两个 batch 实例 —— 从而只能保证 at least once 的语义

- Direct 方式，不基于 Receiver
由 Spark Streaming 直接管理 offset —— 可以给定 offset 范围，直接去 Kafka 的硬盘上读数据，使用 Spark Streaming 自身的均衡来代替 Kafka 做的均衡

这样可以保证，每个 offset 范围属于且只属于一个 batch，从而保证 exactly-once

Direct 的方式，归根结底是由 Spark Streaming 框架来负责整个 offset 的侦测、batch 分配、实际读取数据；并且这些分 batch 的信息都是 checkpoint 到可靠存储（一般是 HDFS）了。这就没有用到 Kafka 使用 ZooKeeper 来均衡 consumer 和记录 offset 的功能，而是把 Kafka 直接当成一个底层的文件系统来使用

## 忽略
应用的实时性需求大于准确性

- 粗粒度忽略
如果计算任务试图读取丢失的源头数据时出错，会导致部分 task 计算失败，会进一步导致整个 batch 的 job 失败，最终在 driver 端以 SparkException 的形式报出来,catch 住这个 SparkException,忽略掉整个 batch 的计算结果

- 细粒度忽略
将忽略部分局限在丢失的  block 上，其它部分继续保留

task 发现作为源数据的 block 失效后，不是直接报错，而是另外生成一个空集合作为“修正”了的源头数据，然后继续 task 的计算，并将成功

# Driver 端长时容错详解

## ReceivedBlockTracker 容错详解
块数据的 meta 信息上报到 ReceiverTracker，然后交给 ReceivedBlockTracker 做具体的管理。ReceivedBlockTracker 也采用 WAL 冷备方式进行备份，在 driver 失效后，由新的 ReceivedBlockTracker 读取 WAL 并恢复 block 的 meta 信息。

有 3 种消息 —— BlockAdditionEvent, BatchAllocationEvent, BatchCleanupEvent —— 会被保存到 WAL 里。

##  DStream, JobGenerator 容错详解
定时对 DStreamGraph 和 JobScheduler 做 Checkpoint，来记录整个 DStreamGraph 的变化、和每个 batch 的 job 的完成情况。

Checkpoint 发起的间隔默认的是和 batchDuration 一致；即每次 batch 发起、提交了需要运行的 job 后就做 Checkpoint，另外在 job 完成了更新任务状态的时候再次做一下 Checkpoint。

checkPoint地点
1. JobGenerator.generateJobs(),JobScheduler 提交jobset异步执行后
2. JobScheduler.clearMetadata(),JobScheduler 成功执行完了提交过来的 JobSet 后，就可以清除此 batch 的相关信息了,先 clear 各种信息,然后发送 DoCheckpoint 消息，触发 doCheckpoint()，就会记录下来我们已经做完了一个 batch

checkpoint内容
```
val checkpointTime: Time
val master: String = ssc.sc.master
val framework: String = ssc.sc.appName
val jars: Seq[String] = ssc.sc.jars
val graph: DStreamGraph = ssc.graph // 【重要】
val checkpointDir: String = ssc.checkpointDir
val checkpointDuration: Duration = ssc.checkpointDuration
val pendingTimes: Array[Time] = ssc.scheduler.getPendingTimes().toArray // 【重要】
val delaySeconds: Int = MetadataCleaner.getDelaySeconds(ssc.conf)
val sparkConfPairs: Array[(String, String)] = ssc.conf.getAll
```

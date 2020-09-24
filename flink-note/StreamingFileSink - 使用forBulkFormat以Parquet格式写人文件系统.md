# 使用forBulkFormat以Parquet格式写人文件系统

## 1. 版本信息

```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <flink.version>1.11.2</flink.version>
    <scala.binary.version>2.12</scala.binary.version>
    <scala.version>2.12.12</scala.version>
</properties>
```

## 2. 代码信息

```java
DataStream<SensorReading> input = env.addSource(new SensorSource());
StreamingFileSink<SensorReading> sink = StreamingFileSink
    .forBulkFormat(new Path("."), ParquetAvroWriters.forReflectRecord(SensorReading.class))
    .withBucketAssigner(new DateTimeBucketAssigner<>("yyyyMMdd")).build();
input.addSink(sink);
env.execute();
```

## 3. POM信息

```xml
<!-- ========== bulk write parquet BEGIN ========== -->
<!-- parquet -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-parquet_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
<!-- avro -->
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-avro</artifactId>
    <version>1.11.1</version>
</dependency>
<!-- hadoop client -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>${hadoop.version}</version>
    <scope>provided</scope>
</dependency>
<!-- ========== bulk write parquet END ========== -->
```

## 4. 故障表现

- 程序卡死，最后打印的信息如下，目录中生成临时文件，但未写入数据。
```text
15:44:28,524 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator     - Triggering checkpoint 1 (type=CHECKPOINT) @ 1600933468517 for job bd5613c30569d8009fb2e9cf628bca7b.
15:44:28,533 INFO  org.apache.flink.streaming.api.functions.sink.filesystem.Buckets  - Subtask 0 checkpointing for checkpoint with id=1 (max part counter=1).
15:44:28,533 INFO  org.apache.parquet.hadoop.InternalParquetRecordWriter         - Flushing mem columnStore to file. allocated memory: 9480
```

- 等待足够长的时间，打印如下信息，系统进行故障恢复，又卡死在上一个打印中
```text
15:47:45,141 WARN  org.apache.flink.runtime.taskmanager.Task                     - Source: Custom Source -> Sink: Unnamed (1/1) (4c1ecabf36a512433cf219f1d7c6c259) switched from RUNNING to FAILED.
java.lang.AbstractMethodError: org.apache.parquet.hadoop.ColumnChunkPageWriteStore$ColumnChunkPageWriter.writePage(Lorg/apache/parquet/bytes/BytesInput;IILorg/apache/parquet/column/statistics/Statistics;Lorg/apache/parquet/column/Encoding;Lorg/apache/parquet/column/Encoding;Lorg/apache/parquet/column/Encoding;)V
	at org.apache.parquet.column.impl.ColumnWriterV1.writePage(ColumnWriterV1.java:53)
	at org.apache.parquet.column.impl.ColumnWriterBase.writePage(ColumnWriterBase.java:315)
```

## 5. 问题原因

相关的jar包由于多个版本共存，产出了错误依赖。

## 6. 修复方法

```xml
<!-- ========== bulk write parquet BEGIN ========== -->
<!-- parquet -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-parquet_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
    <!-- 这里的exclusion很关键，不然会导致程序无法写文件，卡死 -->
    <exclusions>
        <exclusion>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-hadoop</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<!-- avro -->
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-avro</artifactId>
    <version>1.11.1</version>
</dependency>
<!-- hadoop client -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>${hadoop.version}</version>
    <scope>provided</scope>
</dependency>
<!-- ========== bulk write parquet END ========== -->
```

## 7. 正确表现

目录中的文件滚动生成，日志消息循环打印如下内容
```text
15:53:47,848 INFO  org.apache.flink.runtime.taskmanager.Task                     - Source: Custom Source -> Sink: Unnamed (1/1) (2dd4b1e016bf407202f2ea465915ec3d) switched from DEPLOYING to RUNNING.
15:53:47,851 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Custom Source -> Sink: Unnamed (1/1) (2dd4b1e016bf407202f2ea465915ec3d) switched from DEPLOYING to RUNNING.
15:53:53,373 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator     - Triggering checkpoint 1 (type=CHECKPOINT) @ 1600934033367 for job 8efa8c28b50a4a75c223918661369a5f.
15:53:53,382 INFO  org.apache.flink.streaming.api.functions.sink.filesystem.Buckets  - Subtask 0 checkpointing for checkpoint with id=1 (max part counter=1).
15:53:53,815 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator     - Completed checkpoint 1 for job 8efa8c28b50a4a75c223918661369a5f (610 bytes in 446 ms).
15:53:53,817 INFO  org.apache.flink.streaming.api.functions.sink.filesystem.Buckets  - Subtask 0 received completion notification for checkpoint with id=1.
15:53:59,365 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator     - Triggering checkpoint 2 (type=CHECKPOINT) @ 1600934039365 for job 8efa8c28b50a4a75c223918661369a5f.
15:53:59,366 INFO  org.apache.flink.streaming.api.functions.sink.filesystem.Buckets  - Subtask 0 checkpointing for checkpoint with id=2 (max part counter=2).
15:53:59,380 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator     - Completed checkpoint 2 for job 8efa8c28b50a4a75c223918661369a5f (610 bytes in 13 ms).
15:53:59,380 INFO  org.apache.flink.streaming.api.functions.sink.filesystem.Buckets  - Subtask 0 received completion notification for checkpoint with id=2.
```
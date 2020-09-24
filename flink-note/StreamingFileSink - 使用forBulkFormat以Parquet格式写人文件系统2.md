# 使用forBulkFormat以Parquet格式写人文件系统2

## 注意事项A

```text
Bulk Formats can only have `OnCheckpointRollingPolicy`, which rolls (ONLY) on every checkpoint.
```
对于块类型的输出，只能使用`OnCheckpointRollingPolicy`策略滚动生成块文件。

该策略会在每次检查点生成时进行块文件的关闭，新检查点周期创建新文件进行写入。

即：检查点周期=文件创建周期，写入任务并行度=周期内文件数。

## 注意事项B

```text
When using Hadoop < 2.7, please use the OnCheckpointRollingPolicy which rolls part files on every checkpoint.
The reason is that if part files “traverse” the checkpoint interval,
then, upon recovery from a failure the StreamingFileSink may use the truncate() method of
the filesystem to discard uncommitted data from the in-progress file.
This method is not supported by pre-2.7 Hadoop versions and Flink will throw an exception.
```
当`Hadoop`的版本低于2.7时，块类型和行类型的写入都要使用`OnCheckpointRollingPolicy`来确保生成的文件不会跨检查点。

高版本的`Hadoop`可以处理生成文件跨检查点时的容错处理，使用`truncate()`函数进行截断操作，在文件中丢弃未提交数据。
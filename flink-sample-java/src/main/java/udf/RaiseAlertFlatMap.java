package udf;

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import util.Alert;
import util.SensorReading;
import util.SmokeLevel;

public class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {
    private SmokeLevel smokeLevel = SmokeLevel.LOW;
    /*
    类似于流和表的Join：
        算子本地缓存维护了其中一个流的快照信息（这里就是smokeLevel)，并在流消息到达时更新本地快照
        另外一个流则需要借助本地快照的内容对消息进行处理，不需要关系快照更新流的运行情况
        也就是两条流其实是不存在相关性的，两者信息的交互通过本地缓存中的流快照信息来进行
   */
    @Override
    public void flatMap1(SensorReading sensorReading, Collector<Alert> collector) throws Exception {
        if (this.smokeLevel == SmokeLevel.HIGH && sensorReading.temperature > 100) {
            collector.collect(new Alert("Risk of fire! " + sensorReading, sensorReading.timestamp));
        }
    }
    @Override
    public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> collector) throws Exception {
        this.smokeLevel = smokeLevel;
    }
}

package datastream;

import datastream.source.SensorSource;
import datastream.util.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

public class TransformMapOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());

//        DataStream<Long> stream = source.map(value -> value.timestamp);
        DataStream<Long> stream = source.transform("Map",
                TypeExtractor.getMapReturnTypes(env.clean(new MapSensorReadingToTimestamp()), source.getType(), Utils.getCallLocationName(), true),
                new StreamMap<>(env.clean(new MapSensorReadingToTimestamp())));

        stream.print();

        env.execute();
    }
}

class MapSensorReadingToTimestamp implements MapFunction<SensorReading, Long> {
    @Override
    public Long map(SensorReading value) throws Exception {
        return value.timestamp;
    }
}

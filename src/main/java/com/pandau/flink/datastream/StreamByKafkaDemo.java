package com.pandau.flink.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class StreamByKafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.16.9.96:9092");
        props.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink0", new SimpleStringSchema(), props);
        consumer.setStartFromLatest();
        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromGroupOffsets();
        env.addSource(consumer).flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                        String value = s.trim();
                        if (value.contains(" ")){
                            for (String s1 : value.split(" ")) {
                                Tuple2<String, Integer> tuple2 = new Tuple2<>(s1, 1);
                                collector.collect(tuple2);
                            }
                            return;
                        }
                        if (value.contains(",")){
                            for (String s1 : value.split(",")) {
                                Tuple2<String, Integer> tuple2 = new Tuple2<>(s1, 1);
                                collector.collect(tuple2);
                            }
                            return;
                        }
                    }
                }).keyBy(0).sum(1).print();
        env.execute("flink-wordcount");
    }
}

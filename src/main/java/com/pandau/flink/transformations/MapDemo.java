package com.pandau.flink.transformations;

import com.pandau.flink.PathUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile(PathUtils.rootPath+"map-file");

        data.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                if (s.isEmpty()){
                    return 0;
                }
                return Integer.parseInt(s);
            }
        }).reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer integer, Integer t1) throws Exception {
                return integer+t1;
            }
        }).print();

    }
}
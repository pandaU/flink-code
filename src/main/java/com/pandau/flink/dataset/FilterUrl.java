package com.pandau.flink.dataset;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FilterUrl {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> data = env.readTextFile("hdfs://hadoop5:9000/in/file1");

        data.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        return value.startsWith("http://");
                    }
                })
                .writeAsText("hdfs://hadoop5:9000/out/flink-out1");

        JobExecutionResult res = env.execute("wordCount");
    }
}

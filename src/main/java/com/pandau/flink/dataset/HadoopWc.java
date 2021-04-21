package com.pandau.flink.dataset;




import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.IOException;

public class HadoopWc {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile("hdfs://localhost:9000/me");
        data.print();
        data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
                .writeAsText("C:\\Users\\13202\\Desktop\\springCloud\\code_cloud\\flink-code\\src\\resources\\file-hdfs-out");

        JobExecutionResult res = env.execute();
    }
}

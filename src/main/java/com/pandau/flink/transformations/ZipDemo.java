package com.pandau.flink.transformations;

import com.pandau.flink.PathUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

public class ZipDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        DataSource<String> source = env.fromElements("A", "A", "B", "C", "D", "F", "F");
        DataSet<Tuple2<Long, String>> set = DataSetUtils.zipWithUniqueId(source);
        set.writeAsCsv(PathUtils.rootPath+"out-uni", "\n", ",");
        env.execute();
    }
}

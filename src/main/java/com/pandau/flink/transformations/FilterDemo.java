package com.pandau.flink.transformations;

import com.pandau.flink.PathUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FilterDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile(PathUtils.rootPath+"filter-file");
        data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.equals("whx");
            }
        }).print();
    }

}

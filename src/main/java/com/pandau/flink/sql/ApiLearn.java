package com.pandau.flink.sql;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;
public class ApiLearn {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSource<User> dataSource = env.fromElements(User.of("panda", 11), User.of("whx", 10));
        tEnv.createTemporaryView("user",dataSource);
        Table user = tEnv.from("user").where($("name").isEqual("panda"));
        DataSet<User> result = tEnv.toDataSet(user, User.class);
        result.print();
    }
}

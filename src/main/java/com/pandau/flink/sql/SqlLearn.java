package com.pandau.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import static org.apache.flink.table.api.Expressions.*;
public class SqlLearn {
    public static void main(String[] args) throws Exception {
        int count = 0;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT) WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'orders',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");

// execute SELECT statement
        TableResult tableResult1 = tableEnv.executeSql("SELECT * FROM Orders");
// use try-with-resources statement to make sure the iterator will be closed automatically
        /*try (CloseableIterator<Row> it = tableResult1.collect()) {
            while(it.hasNext()) {
                if (count>10){
                   break;
                }
                Row row = it.next();
                String string = row.toString();
                System.out.println(string);
                count++;
            }
        }*/
        // execute Table=
        TableResult tableResult2 = tableEnv.sqlQuery("SELECT * FROM Orders").execute();
        tableResult2.print();
    }
}

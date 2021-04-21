package com.pandau.flink.transformations;

import com.pandau.flink.Store;
import com.pandau.flink.User;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinDemo {

    public static void main(String[] args) throws Exception {
        LocalEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        User user = User.of("pandau",1);
        Store store = Store.of(23, 1);
        DataSet<User> input1 = env.fromElements(user);
        DataSet<Store> input2 =env.fromElements(store);
        // result dataset is typed as Tuple2
        JoinOperator<User, Store, Tuple2<String, Integer>> with = input1.leftOuterJoin(input2)
                .where("zip")       // key of the first input (users)
                .equalTo("zip")
                .with(new JoinFunction<User, Store, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> join(User user, Store store) throws Exception {
                        if (store == null){
                            return new Tuple2<>(user.name, user.zip);
                        }
                        return new Tuple2<>(user.name + "-" + store.age, user.zip);
                    }
                });// key of the second input (stores)
        with.project(0,1).print();
    }

}


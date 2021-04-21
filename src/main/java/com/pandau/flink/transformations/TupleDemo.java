package com.pandau.flink.transformations;


import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class TupleDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        //T3 ->T2的转换
        DataSet<Tuple3<Integer, Double, String>> in =env.fromElements(new Tuple3<Integer, Double, String>(1,2.20,"pandau"),new Tuple3<Integer, Double, String>(2,2.20,"pandau"),new Tuple3<Integer, Double, String>(2,2.20,"whx"));
        // converts Tuple3<Integer, Double, String> into Tuple2<String, Integer>
        DataSet<Tuple2<String, Integer>> out = in.project(2,0);
        out.print();
        //hint   提示
        DataSet<Tuple1<String>> ds2 = in.<Tuple1<String>>project(0).distinct(0);
        ds2.print();
        DataSet<Tuple2<Integer, String>> ds3 = in.project(0,2);
        ds3.print();
        ds3.groupBy(1)
                .sortGroup(0,Order.DESCENDING)
                // sort groups on second tuple field
                .reduceGroup(new DistinctReduce()).print();

        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(new Tuple3<Integer, String, Double>(1,"xx",2.00),new Tuple3<Integer, String, Double>(5,"xxh",4.00),new Tuple3<Integer, String, Double>(3,"xxh",4.00),new Tuple3<Integer, String, Double>(2,"xx",0.00));
        DataSet<Tuple3<Integer, String, Double>> output = input
                .groupBy(1)   // group DataSet on second field
                .combineGroup(new GroupCombineFunction<Tuple3<Integer, String, Double>, Tuple3<Integer, String, Double>>() {
                    @Override
                    public void combine(Iterable<Tuple3<Integer, String, Double>> iterable, Collector<Tuple3<Integer, String, Double>> collector) throws Exception {
                        iterable.forEach(x->{
                            collector.collect(new Tuple3<Integer, String, Double>(x.f0,x.f1,x.f2));
                        });
                    }
                }); //
        System.out.println("///-----------");
        output.print();
        System.out.println("///-----------");
        input.groupBy(1).sortGroup(0,Order.ASCENDING).first(2).print();
        System.out.println("///-----------");
        input.partitionByRange(1).sortPartition(0,Order.DESCENDING).map(new MapFunction<Tuple3<Integer, String, Double>, Tuple3<Integer, String, Double>>() {
            @Override
            public Tuple3<Integer, String, Double> map(Tuple3<Integer, String, Double> integerStringDoubleTuple3) throws Exception {
                System.out.println("线程id#"+Thread.currentThread().getName());
                return integerStringDoubleTuple3;
            }
        }).print();
    }
}
 class DistinctReduce
        implements GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

    @Override
    public void reduce(Iterable<Tuple2<Integer, String>> in, Collector<Tuple2<Integer, String>> out) {
        Integer key = null;
        String comp = null;

        for (Tuple2<Integer, String> t : in) {
            key = t.f0;
            String next = t.f1;

            // check if strings are different
            if (comp == null || !next.equals(comp)) {
                out.collect(new Tuple2<Integer, String>(key, next));
                comp = next;
            }
        }
    }
}

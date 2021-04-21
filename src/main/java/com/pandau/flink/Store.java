package com.pandau.flink;

public class Store {
    public int age; public int zip;
    public static Store of(int age ,int zip){
        Store store = new Store();
        store.age=age;
        store.zip=zip;
        return  store;
    }
    @Override
    public String toString() {
        return "Store{" +
                "age=" + age +
                ", zip=" + zip +
                '}';
    }
}

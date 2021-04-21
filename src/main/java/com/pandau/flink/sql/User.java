package com.pandau.flink.sql;

public class User {
    public String name;
    public int age;
    public static User of(String name,int age){
        User user = new User();
        user.age = age;
        user.name = name;
        return user;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}

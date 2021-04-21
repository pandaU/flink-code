package com.pandau.flink;

public class User {
    public String name; public int zip;
    public static User of(String name ,int zip){
        User user = new User();
        user.name=name;
        user.zip=zip;
        return  user;
    }
    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", zip=" + zip +
                '}';
    }
}

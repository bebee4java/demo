package com.java.spark.sql;

import java.io.Serializable;

/**
 * Created by sgr on 2018/2/21/021.
 */
public class Student implements Serializable {
    private String id;
    private String name;
    private int age;

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "age=" + age +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}

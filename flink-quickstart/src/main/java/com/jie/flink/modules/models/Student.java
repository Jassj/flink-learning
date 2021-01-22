package com.jie.flink.modules.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: Student实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student {
    private int id;
    private String name;
    private String password;
    private int age;

    public int hashCode() {
        return super.hashCode();
    }

    public Student(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public static void main(String[] args) {
        System.out.println(new Student("A", 20).hashCode());
        System.out.println(new Student("B", 21).hashCode());
    }

}

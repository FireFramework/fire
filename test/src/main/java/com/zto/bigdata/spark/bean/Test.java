package com.zto.bigdata.spark.bean;

import static spark.Spark.get;

public class Test {
    public static void main(String[] args) {
        get("/hello", (req, res) -> "Hello World");
    }
}

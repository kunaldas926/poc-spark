package com.antelope.com;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Main {

    private static final String ACCESS = "AKIAT26XFLXSRSJNTQWH";
    private static final String SECRET = "TmUclefSPzY/ZG5BY0eK50K3eEkQcQ4pBnfLyEKZ";

    public static void main(String[] args) {
	// write your code here
        SparkConf conf = new SparkConf().setAppName("S3ReadTest")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.access.key", ACCESS)
                .set("spark.hadoop.fs.s3a.secret.key", SECRET)
//                .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                .setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf)
//                .config("spark.hadoop.fs.s3.awsAccessKeyId", ACCESS)
//                .config("spark.hadoop.fs.s3.awsSecretAccessKey", SECRET)
                .getOrCreate();
//        String s3Url = "s3a://" + ACCESS + ":" + SECRET + "@spark-calcite-demo-bucket/sample.txt";

        String s3aUrl = "s3a://spark-calcite-demo-bucket/sample.txt";
        Dataset<String> data = spark.read().textFile(s3aUrl);
        List<String> lines = data.javaRDD().collect();
        for (String line : lines) {
            System.out.println(line);
        }
        sc.stop();

    }
}

package dev.kunal;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import net.spy.memcached.MemcachedClient;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@Slf4j

public class Main {

    private static final String S3_BUCKET_NAME = "spark-calcite-demo-bucket";
    private static final String S3_SEPARATOR = "/";
    private static final String PROTOCOL = "s3a://";
    private static final String SPARK_PROTOCOL = "spark://";
    private static final String OUTPUT_FOLDER = "output";
    private static final String MEMCACHED_HOST = "localhost";
    private static final Integer MEMCACHED_PORT = 4040;
    private static final Integer SPARK_MASTER_PORT = 4040;

    public static void main(String[] args)  {

        String ip = null;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
            System.out.println("Current IP address : " + ip);
        } catch (UnknownHostException e) {
            ip = "localhost";
        }
        String masterIp = SPARK_PROTOCOL.concat(ip).concat(":").concat(String.valueOf(SPARK_MASTER_PORT));

        SparkConf sparkConf = new SparkConf()
                .setAppName(Main.class.getName())
                .setMaster(masterIp);

        JavaSparkContext javaSparkContext = null;
        MemcachedClient memcachedClient = null;
        try {
            javaSparkContext = new JavaSparkContext(sparkConf);
            memcachedClient = new MemcachedClient(new InetSocketAddress(MEMCACHED_HOST, MEMCACHED_PORT));


            SparkSession sparkSession = SparkSession.builder()
                    .config(sparkConf)
                    .getOrCreate();

            AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.AP_SOUTH_1)
                    .build();

            ListObjectsV2Result listObjectsV2Result = amazonS3.listObjectsV2(S3_BUCKET_NAME);
            List<S3ObjectSummary> s3ObjectSummaries = listObjectsV2Result.getObjectSummaries();

            while (listObjectsV2Result.isTruncated()) {
                listObjectsV2Result = amazonS3.listObjectsV2(listObjectsV2Result
                        .getNextContinuationToken());
                s3ObjectSummaries.addAll(listObjectsV2Result.getObjectSummaries());
            }
            for (String key : s3ObjectSummaries.stream()
                    .map(S3ObjectSummary::getKey)
                    .toArray(String[]::new)) {
                if (!key.contains(OUTPUT_FOLDER)) {
                    Dataset<String> dataset = sparkSession.read()
                            .textFile(
                                    PROTOCOL.concat(S3_BUCKET_NAME)
                                            .concat(S3_SEPARATOR)
                                            .concat(key)
                            );

                    JavaRDD<String> javaRDD = javaSparkContext.parallelize(dataset.collectAsList());
                    javaRDD.flatMap(new FlatMapFunction<String, String>() {
                                @Override
                                public Iterator<String> call(String s) throws Exception {
                                    return Arrays.asList(s.split(" ")).iterator();
                                }
                            }).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum)
                            .saveAsTextFile(
                                    PROTOCOL.concat(S3_BUCKET_NAME)
                                            .concat(S3_SEPARATOR)
                                            .concat(OUTPUT_FOLDER)
                                            .concat(S3_SEPARATOR)
                                            .concat(key)
                            );
                    Objects.requireNonNull(memcachedClient).set(key, 0, javaRDD.collect());
//                    log.info(memcachedClient.get(key).toString());
                    log.info(javaRDD.collect().toString());
                    log.info(String.valueOf(javaRDD.count()));
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            if (Objects.nonNull(javaSparkContext))
                javaSparkContext.close();
            if (Objects.nonNull(memcachedClient))
                memcachedClient.shutdown();
        }
    }
}

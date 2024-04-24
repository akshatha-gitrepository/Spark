package ReduceTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDReduceTest {

    private final SparkConf sparkConf = new SparkConf().setAppName("RDDReduceTest")
            .setMaster("local[*]");
    private static final List<Double> data = new ArrayList<>();
    private final int noOfIterations=10;

    @BeforeAll
    static void BeforeAll() {
        final var dataSize = 1_000_00;
        for (int i = 0; i < dataSize; i++) {
            data.add(100 * ThreadLocalRandom.current().nextDouble() + 47);
        }
        assertEquals(dataSize, data.size());
    }


    @Test
    void testReduceActionUsingSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRDD = (sparkContext.parallelize(data));
            final Instant start = Instant.now();
            for(int i=0;i<noOfIterations;i++){
                final var sum=myRDD.reduce(Double::sum);
                System.out.println("[Spark RDD reduce] SUM :" + sum);
            }

            final long timeElapsed = (Duration.between(start,Instant.now()).toMillis())/noOfIterations;
            System.out.printf("[Spark RDD reduce] time taken : %d ms%n%n",timeElapsed);
        }
    }

    @Test
    void testFoldActionUsingSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRDD = (sparkContext.parallelize(data));
            final Instant start = Instant.now();
            for(int i=0;i<noOfIterations;i++){
                final var sum=myRDD.fold(0D,Double::sum);
                System.out.println("[Spark Fold reduce] SUM :" + sum);
            }

            final long timeElapsed = (Duration.between(start,Instant.now()).toMillis())/noOfIterations;
            System.out.printf("[Spark RDD Fold] time taken : %d ms%n%n",timeElapsed);
        }
    }

    @Test
    @DisplayName("Test aggregate() method in Spark RDD")
    void testAggregateActionUsingSparkRDD() {
        try (final var sparkContext = new JavaSparkContext(sparkConf)) {
            final var myRDD = (sparkContext.parallelize(data));
            final Instant start = Instant.now();
            for(int i=0;i<noOfIterations;i++){
                final var sum=myRDD.aggregate(0D,Double::sum,Double::sum);
                final var max=myRDD.aggregate(0D,Double::sum,Double::sum);
                final var min=myRDD.aggregate(0D,Double::sum,Double::min);
                System.out.println("[Spark Fold Aggregate] SUM :" + sum);
                System.out.println("[Spark Fold Aggregate] MAX for all partitions :" + max);
                System.out.println("[Spark Fold Aggregate] MIN for all partitions :" + min);
            }

            final long timeElapsed = (Duration.between(start,Instant.now()).toMillis())/noOfIterations;
            System.out.printf("[Spark RDD Fold] time taken : %d ms%n%n",timeElapsed);
        }
    }
}


package RDDMappingTest;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDMappingTest {
    private final SparkConf conf = new SparkConf().setAppName("CreateRDDMapTest")
            .setMaster("local[*]");

    private static final List<String> data = new ArrayList<>();
    private final int noOfIteration=10;

    @BeforeAll
    static  void beforeAlll(){
        final var dataSize=100_000;
        for(int i=0;i<dataSize;i++) {
            data.add(RandomStringUtils.randomAscii(ThreadLocalRandom.current().nextInt(10)));
        }
        assertEquals(dataSize,data.size());
        }

        @Test
        @DisplayName("Test map operation using Spark RDD count() method")
        void testMapOperationUsingSparkRDDCount(){
         try(final var sparkContext = new JavaSparkContext(conf)) {
             JavaRDD<String> myRDD = sparkContext.parallelize(data);

             final Instant start = Instant.now();
             for (int i = 0; i < noOfIteration; i++) {
                    final var strLengths= myRDD.map(String::length).count();
                    assertEquals(data.size(),strLengths);

             }

            final long timeElapsed = (Duration.between(start,Instant.now()).toMillis())/ noOfIteration;
             System.out.printf("[Spark RDD]count method : %d ms%n%n",timeElapsed );
         }
        }

    @Test
    @DisplayName("Test map operation using Spark RDD mapReduce")
    void testMapOperationUsingSparkRDDMapReduce() {
        try (final var sparkContext = new JavaSparkContext(conf)) {
            final var myRdd = sparkContext.parallelize(data);

            final Instant start = Instant.now();
            for (int i = 0; i < noOfIteration; i++) {
                final var strLengths = myRdd.map(String::length)
                        .map(v -> 1L)
                        .reduce(Long::sum);
                System.out.println(strLengths);
                assertEquals(data.size(), strLengths);
            }
            final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIteration;
            System.out.printf("[Spark RDD] mapReduce time taken: %d ms%n%n", timeElapsed);
        }
    }

    @Test
    @DisplayName("Test map operation using Java Streams")
    void testMapOperationUsingJavaStreams() {
        final Instant start = Instant.now();
        for (int i = 0; i < noOfIteration; i++) {
            final var strLengths = data.stream()
                    .map(String::length)
                    .collect(Collectors.toList());

         //   System.out.println(strLengths);
            assertEquals(data.size(), strLengths.size());
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIteration;
        System.out.printf("[Java Streams] time taken: %d ms%n%n", timeElapsed);
    }

    @Test
    @DisplayName("Test map operation using Java Parallel Streams")
    void testMapOperationUsingJavaParallelStreams() {
        final Instant start = Instant.now();
        for (int i = 0; i < noOfIteration; i++) {
            final var strLengths = data.parallelStream()
                    .map(String::length)
                    .collect(Collectors.toList());
            System.out.println();
            assertEquals(data.size(), strLengths.size());
        }
        final long timeElapsed = (Duration.between(start, Instant.now()).toMillis()) / noOfIteration;
        System.out.printf("[Java Parallel Streams] time taken: %d ms%n%n", timeElapsed);
    }


    }



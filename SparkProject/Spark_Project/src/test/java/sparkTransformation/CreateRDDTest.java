package sparkTransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CreateRDDTest {

     private final SparkConf conf = new SparkConf().setAppName("CreateRDDTest")
             .setMaster("local[*]");

     @Test
     @DisplayName("create an empty RDD with no partitions")
     void createAnEmptyRDD() {
          try (final var sparkContext = new JavaSparkContext(conf)) {
               final var javaRDD = sparkContext.emptyRDD();
               System.out.println(javaRDD);
               System.out.printf("no of partitions: %d%n", javaRDD.getNumPartitions());

          }

     }

     @Test
     @DisplayName("create an empty RDD with default partitions")
     void createAnEmptyRDDWithDefaultPartition() {
          try (final var sparkContext = new JavaSparkContext(conf)) {
               final var javaRDD = sparkContext.parallelize(List.of());
               System.out.println(javaRDD);
               System.out.printf("no of partitions: %d%n", javaRDD.getNumPartitions());

          }

     }

     @Test
     @DisplayName("create an Java RDD with parllelize method")
     void createAnJavaRDD() {
          try (final var sparkContext = new JavaSparkContext(conf)) {
               final var collect = Stream.iterate(1, n -> n + 1).limit(10).collect(Collectors.toList());

               final var javaRDD = sparkContext.parallelize(collect);
               System.out.println(javaRDD);
               System.out.printf("no of partitions: %d%n", javaRDD.getNumPartitions());
               System.out.printf("no of elements: %d%n", javaRDD.count());
               System.out.println("Elements in RDD");
               javaRDD.collect().forEach(System.out::println);

               //reduce operations
               final var max = javaRDD.reduce(Integer::max);
               final var min = javaRDD.reduce(Integer::min);
               final var sum = javaRDD.reduce(Integer::sum);
               System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);

          }


     }

     @Test
     @DisplayName("create an Java RDD with parllelize method, and no.of partition passed as arguement")
     void createAnJavaRDDWithPartitionPassed() {
          try (final var sparkContext = new JavaSparkContext(conf)) {
               final var collect = Stream.iterate(1, n -> n + 1).limit(10).collect(Collectors.toList());

               final var javaRDD = sparkContext.parallelize(collect, 8);
               System.out.println(javaRDD);
               System.out.printf("no of partitions: %d%n", javaRDD.getNumPartitions());
               System.out.printf("no of elements: %d%n", javaRDD.count());
               System.out.println("Elements in RDD");
               javaRDD.collect().forEach(System.out::println);

               //reduce operations
               final var max = javaRDD.reduce(Integer::max);
               final var min = javaRDD.reduce(Integer::min);
               final var sum = javaRDD.reduce(Integer::sum);
               System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);

          }

     }
}
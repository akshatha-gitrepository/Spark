package sparkTransformation;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkFirstProgram {
    public static void main(String[] args) {

try (final var spark = SparkSession.builder()
                .appName("SparkFirstProgram")
                .master("local[*]")
              //  .config("spark.ui.enabled", "true")
                .getOrCreate();
      final var sc = new JavaSparkContext(spark.sparkContext())) {
            final var data = Stream.iterate(1, n -> n + 1)
                    .limit(5)
                    .collect(Collectors.toList());
            data.forEach(System.out::println);
            final var myRdd = sc.parallelize(data);
            System.out.printf("Total elements in RDD: %d%n", myRdd.count());
            System.out.printf("Default number of partitions: %d%n", myRdd.getNumPartitions());

            final var max = myRdd.reduce(Integer::max);
            final var min = myRdd.reduce(Integer::min);
            final var sum = myRdd.reduce(Integer::sum);
            System.out.printf("MAX~>%d, MIN~>%d, SUM~>%d%n", max, min, sum);

            try (final var scanner = new Scanner(System.in)) {
                scanner.nextLine();
            }
        }

     }


}

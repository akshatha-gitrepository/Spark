package RDDMappingTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RDDFlatMapTest {
    @Test
    @DisplayName("Test flatMap() method in Spark RDD")
    void testFlatMapInSparkRDD() {
        final var conf = new SparkConf().setAppName("RDDFlatMapsTest").setMaster("local[*]");
        try (final var sc = new JavaSparkContext(conf)) {
            final String testFilePath = Path.of("src", "test", "resources", "magna-carta.txt.gz").toString();
            final var lines = sc.textFile(testFilePath);
            System.out.printf("Total lines in file %d%n", lines.count());

            final var mappedLines = lines.map(line -> List.of(line.split("\\s")));
            mappedLines.take(10).forEach(System.out::println);
            System.out.printf("Total lines in file %d%n", mappedLines.count());
            assertEquals(lines.count(), mappedLines.count());

            final var words = lines.flatMap(line -> List.of(line.split("\\s")).iterator());
            words.take(30).forEach(System.out::println);
            System.out.printf("Total number of words in the file~>%d%n", words.count());

            System.out.println("First few words:");

            System.out.println("--------------------");
        }
    }
}
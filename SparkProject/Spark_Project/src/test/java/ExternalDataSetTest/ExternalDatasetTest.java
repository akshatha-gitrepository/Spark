package ExternalDataSetTest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExternalDatasetTest {
    private final SparkConf conf = new SparkConf().setAppName("CreateRDDTest")
            .setMaster("local[*]");

    @ParameterizedTest
    @ValueSource(strings = {
            "C:\\Users\\seetha\\idea-projects\\SparkProject\\src\\test\\resources\\git.txt",
         //   "C:\\Users\\seetha\\idea-projects\\SparkProject\\src\\test\\resources\\git.zip"
    })
    @DisplayName("loading local files to spark RDD")
    public void testLoadingTextFileintoRDD(final String localFilePath){
        try(final var springContext = new JavaSparkContext(conf)){
            final var stringJavaRDD = springContext.textFile(localFilePath);
            System.out.printf("Total lines in the file : %d%n",stringJavaRDD.count());
            System.out.println("Priniting first 10 lines-->");
            stringJavaRDD.take(10).forEach(System.out::println);
            System.out.println("-----------------------------------");

        }

    }

    @ParameterizedTest
    @MethodSource("getFilePaths")
    @DisplayName("loading local files to spark RDD")
    public void testLoadingTextFileintoRDDUsingMethod(final String localFilePath){
        try(final var springContext = new JavaSparkContext(conf)){
            final var stringJavaRDD = springContext.textFile(localFilePath);
            System.out.printf("Total lines in the file : %d%n",stringJavaRDD.count());
            System.out.println("Priniting first 10 lines-->");
            stringJavaRDD.take(20).forEach(System.out::println);
            System.out.println("-----------------------------------");

        }

    }

    @Test
    @DisplayName("Test loading whole directory into Spark RDD")
    public void testLoadingWholeDirectoryIntoSparkRDD(){
        try(final var sparkContext= new JavaSparkContext(conf)){
            final var testDirPath = Path.of("src", "test", "resources").toString();
         final var javaPairRDD = sparkContext.wholeTextFiles(testDirPath);
            System.out.printf("total no.of files in directory : [%s] = %d%n",testDirPath,javaPairRDD.count());
            javaPairRDD.collect().forEach(tuple->{
                System.out.printf("File name : %s%n", tuple._1);
                System.out.println("--------------------------");
                if(tuple._1.endsWith("properties")){
                    System.out.printf("contents of [%s] : %n",tuple._1);
                    System.out.println(tuple._2);
                }
            });

        }
    }

    @Test
    @DisplayName("Test loading file from Amazon S# into spark RDD")
    void testLoadingAmazonSFileIntoSparkRDD(){
        try(final var sparkConetxt = new JavaSparkContext(conf)){
            sparkConetxt.hadoopConfiguration().set("fs.s3a.access.key", "AWS access-key value");
            sparkConetxt.hadoopConfiguration().set("fs.s3a.secret.key", "AWS secret-key value");
            sparkConetxt.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");
            final var myRDD= sparkConetxt.textFile("s3a://backstreetbrogrammer-bucket/1TrillionWords.txt.gz");

             /*System.out.printf("Total lines in file %d%n", myRdd.count());
            System.out.println("Printing first 10 lines~>");
            myRdd.take(10).forEach(System.out::println);*/

            assertThrows(AccessDeniedException.class, myRDD::count);
        }
    }

    @Test
    @DisplayName("Test loading whole CSV file into Spark RDD")
    public void testLoadingCSVFileIntoSparkRDD(){
        try(final var sparkContext= new JavaSparkContext(conf)){
            final var testCSVPath = Path.of("src", "test", "resources","dma.csv").toString();
            final var javaRDD = sparkContext.textFile(testCSVPath);
            System.out.printf("total no.of files in directory : [%s] = %d%n",testCSVPath,javaRDD.count());
            System.out.println("CSV Headers-->");
            System.out.println(javaRDD.first());
            System.out.println("--------------------------");
            final var csvFields =javaRDD.map(line-> line.split(","));
            csvFields.take(5)
                    .forEach(fields-> System.out.println(String.join("|",fields)));
        }
    }

    private static Stream<Arguments> getFilePaths(){
        return Stream.of(
                Arguments.of(Path.of("src","test","resources","git.txt").toString())
        );
    }
}

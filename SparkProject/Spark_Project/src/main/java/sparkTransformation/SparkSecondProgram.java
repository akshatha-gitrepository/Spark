package sparkTransformation;

import com.amazonaws.services.quicksight.model.DataSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class SparkSecondProgram {
    public static void main(String[] args) {

        SparkSession spark= SparkSession.builder()
                .appName("csv to db")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header",true)
                .load("src/test/resources/dma.csv");
      //  df.show();
       df= df.withColumn("ful_data", concat(df.col("region"),lit(","),df.col("state")));

       df=df.filter(df.col("state").rlike("NY")).orderBy(df.col("region").asc());
       df.show();

       String dbConnectionUrl="jdbc:mysql://localhost:3306/csvdata";
        Properties prop = new Properties();
        prop.setProperty("driver","com.mysql.cj.jdbc.Driver");
        prop.setProperty("user","root");
        prop.setProperty("password","root");
        df.write().mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl,"csvdata",prop);

    }
}

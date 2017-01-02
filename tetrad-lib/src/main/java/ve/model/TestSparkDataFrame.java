package ve.model;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class TestSparkDataFrame {

	public static void main(String[] args) {
		Dataset<Row> df = readCsvAsDataFrame(
				"C:\\Users\\mot16\\projects\\master\\data\\joined_data_no_dates_cleanedin_python_discretizedin_weka.csv");
		System.out.println("========== Print Schema ============");
		df.printSchema();
		System.out.println("========== Print Data (Limit 10) ==============");
		df.limit(10).show();
		System.out.println("========== Print title ==============");
		df.select("title").show();

	}

	private static Dataset<Row> readCsvAsDataFrame(String filePath) {
		SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
		// create Spark Context
		SparkContext context = new SparkContext(conf);
		// create spark Session
		SparkSession sparkSession = new SparkSession(context);
		SQLContext sqlContext = new SQLContext(sparkSession);
		Dataset<Row> df = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").load(filePath);
		return df;
	}

}

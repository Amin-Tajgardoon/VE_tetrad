package ve.model;

import java.sql.*;
import org.relique.jdbc.csv.CsvDriver;

public class TestCsvJdbc {
	public static void main(String[] args) throws Exception {
		// Load the driver.
		Class.forName("org.relique.jdbc.csv.CsvDriver");

		// Create a connection. The first command line parameter is
		// the directory containing the .csv files.
		// A single connection is thread-safe for use by several threads.
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + "C:\\Users\\mot16\\projects\\master\\data");

		// Create a Statement object to execute the query with.
		// A Statement is not thread-safe.
		Statement stmt = conn.createStatement();

		// Select the ID and NAME columns from sample.csv
		ResultSet results = stmt
				.executeQuery("SELECT * FROM joined_data_no_dates_cleanedin_python_discretizedin_weka LIMIT 2");

		// Dump out the results to a CSV file with the same format
		// using CsvJdbc helper function
		boolean append = true;
		CsvDriver.writeToCsv(results, System.out, append);

		// Clean up
		conn.close();
	}
}
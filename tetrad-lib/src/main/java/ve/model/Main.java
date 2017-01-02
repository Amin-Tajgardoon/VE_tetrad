package ve.model;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.relique.jdbc.csv.CsvDriver;

import edu.cmu.tetrad.bayes.BayesIm;
import edu.cmu.tetrad.bayes.BayesPm;
import edu.cmu.tetrad.bayes.BayesUpdater;
import edu.cmu.tetrad.bayes.Evidence;
import edu.cmu.tetrad.bayes.MlBayesEstimator;
import edu.cmu.tetrad.bayes.RowSummingExactUpdater;
import edu.cmu.tetrad.data.DataSet;
import edu.cmu.tetrad.data.DiscreteVariable;
import edu.cmu.tetrad.data.Knowledge;
import edu.cmu.tetrad.graph.Graph;
import edu.cmu.tetrad.io.VerticalTabularDiscreteDataReader;
import edu.cmu.tetrad.search.BDeuScore;
import edu.cmu.tetrad.search.Fgs;
import edu.cmu.tetrad.search.SearchGraphUtils;

public class Main {

	public static void main(String[] args) throws Exception {

		// DataSet dataSet = readDataSet();

		String fileDirectory = "C:\\Users\\mot16\\projects\\master\\data\\";
		String fileName = "joined_data_no_dates_cleanedin_python_discretizedin_weka_replaced_age_values";
		//String fileName = "gsk_108134_joined_no_dummies_discrete_age_tempr_final.csv";

		String resultsFilePath = "C:\\Users\\mot16\\projects\\master\\output\\results_tetrad_ve.csv";
		String resultHeader = "contidition,ve,ve_hat" + "\n";
		
		//Files.write(Paths.get(resultsFilePath), resultHeader.getBytes());

		DataSet dataSet = readDataSet(fileDirectory, fileName);
		
		Connection conn = readCsvByJdbc(fileDirectory);

		String targetName = "wviral_rawres";
		String treatmentName = "group_nb";

		// DiscreteVariableAnalysis variableAnalysis =
		// dataReader.getVariableAnalysis();
		// DiscreteVarInfo[] variables = variableAnalysis.getDiscreteVarInfos();

		Knowledge knwlg = create3TierKnowledge();
		//Knowledge knwlg = new Knowledge();

		Graph dagFromPattern = searchForDag(dataSet, knwlg);

		BayesPm pm = new BayesPm(dagFromPattern);

		BayesIm im = new MlBayesEstimator().estimate(pm, dataSet);

		// BayesUpdaterClassifier bayesUpdaterClassifier = new
		// BayesUpdaterClassifier(im, dataSet);
		// bayesUpdaterClassifier.setTarget(targetName, 0);

		DiscreteVariable targetVariable = (DiscreteVariable) im.getNode(targetName);
		int indexTargetBN = im.getNodeIndex(im.getNode(targetName));

		DiscreteVariable treatmentVar = (DiscreteVariable) im.getNode(treatmentName);
		List<String> treatmentCats = treatmentVar.getCategories();

		Evidence evidence;
		BayesUpdater bayesUpdater;

		// Restrict all other variables to their observed values in
		// this case.
		for (int i = 0; i < im.getVariables().size(); i++) {
			if (i == im.getVariables().indexOf(im.getNode(targetName))
					|| i == im.getVariables().indexOf(im.getNode(treatmentName))) {
				continue;
			}

			String jName = im.getVariables().get(i).getName();
			DiscreteVariable jVariable = (DiscreteVariable) im.getVariables().get(i);

			List<String> jCategories = jVariable.getCategories();
			for (String category : jCategories) {
				evidence = Evidence.tautology(im);

				int jIndex = evidence.getNodeIndex(jName);
				evidence.getProposition().setCategory(jIndex, jCategories.indexOf(category));

				double[] marginals = new double[treatmentCats.size()];
				double[] attackCount = new double[marginals.length];
				double[] popCount = new double[marginals.length];
				Statement statement;
				ResultSet result;

				for (int j = 0; j < treatmentCats.size(); j++) {

					evidence.getProposition().setCategory(evidence.getNodeIndex(treatmentName), j);
					// evidence.getProposition().setCategory(itarget,
					// targetVariable.getCategories().indexOf("P"));
					// evidence.getProposition().setVariable(itarget, false);
					bayesUpdater = new RowSummingExactUpdater(im);
					// Update using those values.
					bayesUpdater.setEvidence(evidence);
					marginals[j] = bayesUpdater.getMarginal(indexTargetBN, targetVariable.getCategories().indexOf("P"));

					System.out.println("P(D=P" + " | groub_nb=" + treatmentVar.getCategory(j) + ", " + jName + "="
							+ category + ")= " + marginals[j]);

					statement = conn.createStatement();
					String sql = "select count(*) count from " + fileName + " where wviral_rawres='P' "
							+ " and group_nb=" + "'" + treatmentVar.getCategory(j) + "'" + " and " + jName + "=" + "'"
							+ category + "'";
					result = statement.executeQuery(sql);
					result.next();
					attackCount[j] = result.getDouble(1);
					System.out.println("count(D=P" + ", groub_nb=" + treatmentVar.getCategory(j) + ", " + jName + "="
							+ category + ")= " + attackCount[j]);
					sql = "select count(*) count from " + fileName + " where group_nb=" + "'"
							+ treatmentVar.getCategory(j) + "'" + " and " + jName + "=" + "'" + category + "'";
					result = statement.executeQuery(sql);
					result.next();
					popCount[j] = result.getDouble(1);
					System.out.println("count(groub_nb=" + treatmentVar.getCategory(j) + ", " + jName + "=" + category
							+ ")= " + popCount[j]);
					System.out.println("AttackRate=" + attackCount[j] / popCount[j] + "\n");

				}

				double VEhat = 1 - marginals[0] / marginals[1];
				System.out.println("VE_hat= " + VEhat);
				double VE = 1 - (attackCount[0] / popCount[0]) / (attackCount[1] / popCount[1]);
				System.out.println("actual_VE=" + VE);
				System.out.println("----------------------------------------------------------" + "\n");

				String resultLine = jName + "=" + category + "," + VE + "," + VEhat + "\n";

				writeResultsToFile(resultLine, resultsFilePath);

			}

		}

		System.out.println(dagFromPattern);
		conn.close();

	}

	static Graph searchForDag(DataSet dataSet, Knowledge knwlg) {
		Graph graph = fgs(dataSet, knwlg);
		
		System.out.println(graph);
		
		Graph dagFromPattern = SearchGraphUtils.dagFromPattern(graph);
		return dagFromPattern;
	}

	static DataSet readDataSet(String fileDirectory, String fileName) throws IOException {
		Path dataPath = Paths.get(fileDirectory, fileName + ".csv");

		char delimiter = ',';
		VerticalTabularDiscreteDataReader dataReader = new VerticalTabularDiscreteDataReader(dataPath, delimiter);
		DataSet dataSet = dataReader.readInData();
		return dataSet;
	}

	private static Graph fgs(DataSet dataSet, Knowledge knwlg) {
		BDeuScore bdeu = new BDeuScore(dataSet);

		Fgs fgs = new Fgs(bdeu);
		fgs.setKnowledge(knwlg);
		Graph graph = fgs.search();
		return graph;
	}

	private static Knowledge create3TierKnowledge() {
		Knowledge knwlg = new Knowledge();

		String[] tier1Vars = new String[] { "age", "sex", "cty_cod", "ethnic", "race" };
		String[] tier2Vars = new String[] { "center", "group_nb" };
		String[] tier3Vars = new String[] { "wviral_rawres" };
		for (String var : tier1Vars)
			knwlg.addToTier(1, var);
		for (String var : tier2Vars)
			knwlg.addToTier(2, var);
		for (String var : tier3Vars)
			knwlg.addToTier(3, var);
		return knwlg;
	}

	private static Knowledge create2TierKnowledge() {
		Knowledge knwlg = new Knowledge();

		String[] tier1Vars = new String[] { "age", "sex", "cty_cod", "ethnic", "race", "center", "group_nb" };
		String[] tier2Vars = new String[] { "wviral_rawres" };
		for (String var : tier1Vars)
			knwlg.addToTier(1, var);
		for (String var : tier2Vars)
			knwlg.addToTier(2, var);
		return knwlg;
	}

	private static DataSet readDataSet() throws IOException {
		Path dataPath = Paths.get("C:\\Users\\mot16\\projects\\master\\data", "saved_from_weka_code2.csv");

		char delimiter = ',';
		VerticalTabularDiscreteDataReader dataReader = new VerticalTabularDiscreteDataReader(dataPath, delimiter);
		DataSet dataSet = dataReader.readInData();
		return dataSet;
	}

	static Connection readCsvByJdbc(String fileDirectory) throws Exception {
		Class.forName("org.relique.jdbc.csv.CsvDriver");

		// Create a connection. The first command line parameter is
		// the directory containing the .csv files.
		// A single connection is thread-safe for use by several threads.
		Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + fileDirectory);

		return conn;
	}

	static void writeResultsToFile(String result, String filePath) throws IOException {
		Files.write(Paths.get(filePath), result.getBytes(),
				StandardOpenOption.APPEND);
	}

	// private static void readCsvAsDataFrame(String filePath) {
	// SparkConf conf = new
	// SparkConf().setAppName("JavaWordCount").setMaster("local");
	// // create Spark Context
	// SparkContext context = new SparkContext(conf);
	// // create spark Session
	// SparkSession sparkSession = new SparkSession(context);
	// SQLContext sqlContext = new SQLContext(sparkSession);
	// Dataset<Row> df =
	// sqlContext.read().format("com.databricks.spark.csv").option("inferSchema",
	// "true")
	// .option("header", "true").load(filePath);
	// System.out.println("========== Print Schema ============");
	// df.printSchema();
	// System.out.println("========== Print Data ==============");
	// df.show();
	// System.out.println("========== Print title ==============");
	// df.select("title").show();
	//
	// }

}

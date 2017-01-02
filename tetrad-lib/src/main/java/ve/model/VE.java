package ve.model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import edu.cmu.tetrad.bayes.BayesIm;
import edu.cmu.tetrad.bayes.BayesPm;
import edu.cmu.tetrad.bayes.BayesUpdater;
import edu.cmu.tetrad.bayes.DirichletBayesIm;
import edu.cmu.tetrad.bayes.DirichletEstimator;
import edu.cmu.tetrad.bayes.Evidence;
import edu.cmu.tetrad.bayes.MlBayesEstimator;
import edu.cmu.tetrad.bayes.RowSummingExactUpdater;
import edu.cmu.tetrad.bayes.ApproximateUpdater;
import edu.cmu.tetrad.data.DataSet;
import edu.cmu.tetrad.data.DiscreteVariable;
import edu.cmu.tetrad.data.Knowledge;
import edu.cmu.tetrad.graph.Graph;

public class VE {

	public static void main(String[] args) throws Exception {
		String fileDirectory = "C:\\Users\\mot16\\projects\\master\\data\\final";
		String noDummies = "gsk_108134_joined_no_dummies_discrete_age_tempr_final";
		String full = "gsk_108134_joined_allvars_discrete_age_tempr_final";

		DataSet noDummiesData = Main.readDataSet(fileDirectory, noDummies);
		DataSet fullData = Main.readDataSet(fileDirectory, full);

		Connection conn = Main.readCsvByJdbc(fileDirectory);

		String targetName = "H3";
		String targetCategory = "MATCH";
		String treatmentName = "VACCINE_expogn";

		Knowledge knwlg = new Knowledge();
		String[] classVars = new String[] { "RAWRES", "viral_res_binary", "rt_PCR", "H1", "H3", "B" };
		String[] otherVars = new String[] { "CENTER_wdemog", "RACE_wdemog", "SEX_wdemog", "RDE_SCHD_wdemog",
				"CTY_COD_wdemog", "TREATMNT_wdemog", "AGE_wdemog", "ETHN_NEW_wdemog", "VACCINE_expogn",
				"VIAL_TYP_expogn", "P_AP_expogn", "AGECAT_pid", "SYMP_SID_reaccod", "CODEBRK_wconc", "DROP_OUT_wconc",
				"LC_GC_wconc", "PREGNANT_wconc", "AE_FLAG_wconc", "SAE_CNT_wconc", "RAWRES_wlabo", "TYPE_VAC_wnpap",
				"HISTVCFL_wphist", "SEASON1_wphist", "SEASON2_wphist", "SEASON3_wphist", "OUTCOME_wpneumo",
				"SYMP_VAL_wsolpre", "SYMP_SIT_wsolpre", "any_conc_vac", "any_medicine", "any_diagnosis" };
		for (String var : otherVars)
			knwlg.addToTier(1, var);
		for (String var : classVars)
			knwlg.addToTier(2, var);
		knwlg.setTierForbiddenWithin(2, true);

		DataSet dataSet = noDummiesData;
		String fileNameAsTableName = noDummies;
		String resultsFilePath = "C:\\Users\\mot16\\projects\\master\\output\\results_tetrad_ve_" + fileNameAsTableName
				+ ".csv";
		String resultHeader = "contidition,ve,ve_hat" + "\n";
		Files.write(Paths.get(resultsFilePath), resultHeader.getBytes());

		Graph dagFromPattern = Main.searchForDag(dataSet, knwlg);

		BayesPm pm = new BayesPm(dagFromPattern);

		// BayesIm im = new MlBayesEstimator().estimate(pm, dataSet);

		DirichletBayesIm prior = DirichletBayesIm.symmetricDirichletIm(pm, 0.5);

		System.out.println("PRIOR = 0.5");

		DirichletBayesIm im = DirichletEstimator.estimate(prior, dataSet);

		System.out.println("ESTIMATION FINISHED");

		DiscreteVariable targetVariable = (DiscreteVariable) im.getNode(targetName);
		int indexTargetBN = im.getNodeIndex(im.getNode(targetName));

		DiscreteVariable treatmentVar = (DiscreteVariable) im.getNode(treatmentName);
		List<String> treatmentCats = treatmentVar.getCategories();

		Evidence evidence;
		BayesUpdater bayesUpdater;

		// TEST: calculate some VEs in publication
		double[] probs = new double[treatmentCats.size()];
		for (int i = 0; i < 2; i++) {
			evidence = Evidence.tautology(im);
			evidence.getProposition().setCategory(evidence.getNodeIndex(treatmentName), i);
			// bayesUpdater = new RowSummingExactUpdater(im);
			ApproximateUpdater bayesUpdater2 = new ApproximateUpdater(im);
			bayesUpdater2.setEvidence(evidence);
			int idxOfTargetCat = targetVariable.getCategories().indexOf(targetCategory);
			probs[i] = bayesUpdater2.getMarginal(indexTargetBN, idxOfTargetCat);
			System.out.println("P(" + targetName + "=" + targetCategory + " | " + treatmentName + "="
					+ treatmentVar.getCategory(i) + ")= " + probs[i]);
		}
		// END OF TEST

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
					int idxOfTargetCat = targetVariable.getCategories().indexOf(targetCategory);
					marginals[j] = bayesUpdater.getMarginal(indexTargetBN, idxOfTargetCat);

					System.out.println("P(" + targetName + "=" + targetCategory + " | " + treatmentName + "="
							+ treatmentVar.getCategory(j) + ", " + jName + "=" + category + ")= " + marginals[j]);

					statement = conn.createStatement();
					String sql = "SELECT count(*) count FROM " + fileNameAsTableName + " WHERE " + targetName + "='"
							+ targetCategory + "' " + " AND " + treatmentName + "=" + "'" + treatmentVar.getCategory(j)
							+ "'" + " AND " + jName + "=" + "'" + category + "'";
					result = statement.executeQuery(sql);
					result.next();
					attackCount[j] = result.getDouble(1);
					System.out.println("count(" + targetName + "=" + targetCategory + ", " + treatmentName + "="
							+ treatmentVar.getCategory(j) + ", " + jName + "=" + category + ")= " + attackCount[j]);
					sql = "SELECT count(*) count FROM " + fileNameAsTableName + " WHERE " + treatmentName + "=" + "'"
							+ treatmentVar.getCategory(j) + "'" + " AND " + jName + "=" + "'" + category + "'";
					result = statement.executeQuery(sql);
					result.next();
					popCount[j] = result.getDouble(1);
					System.out.println("count(" + treatmentName + "=" + treatmentVar.getCategory(j) + ", " + jName + "="
							+ category + ")= " + popCount[j]);
					System.out.println("AttackRate=" + attackCount[j] / popCount[j] + "\n");

				}

				double VEhat = 1 - marginals[0] / marginals[1];
				System.out.println("VE_hat= " + VEhat);
				double VE = 1 - (attackCount[0] / popCount[0]) / (attackCount[1] / popCount[1]);
				System.out.println("actual_VE=" + VE);
				System.out.println("----------------------------------------------------------" + "\n");

				String resultLine = jName + "=" + category + "," + VE + "," + VEhat + "\n";

				Main.writeResultsToFile(resultLine, resultsFilePath);

			}

		}

		System.out.println(dagFromPattern);
		conn.close();
	}
}

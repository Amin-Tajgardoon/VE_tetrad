/*
 * Copyright (C) 2016 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.cmu.tetrad.cli.search;

import edu.cmu.tetrad.algcomparison.algorithm.Algorithm;
import edu.cmu.tetrad.algcomparison.algorithm.oracle.pattern.Fgs;
import edu.cmu.tetrad.algcomparison.score.BdeuScore;
import edu.cmu.tetrad.cli.AbstractAlgorithmCli;
import edu.cmu.tetrad.cli.AlgorithmType;
import edu.cmu.tetrad.cli.ParamAttrs;
import edu.cmu.tetrad.cli.util.Args;
import edu.cmu.tetrad.cli.validation.DataValidation;
import edu.cmu.tetrad.cli.validation.LimitDiscreteCategory;
import edu.cmu.tetrad.cli.validation.UniqueVariableNames;
import edu.cmu.tetrad.data.DataSet;
import edu.cmu.tetrad.data.IKnowledge;
import edu.cmu.tetrad.io.DataReader;
import edu.cmu.tetrad.io.VerticalTabularDiscreteDataReader;
import edu.cmu.tetrad.util.ParamDescriptions;
import edu.cmu.tetrad.util.Parameters;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 *
 * Sep 14, 2016 2:31:24 PM
 *
 * @author Kevin V. Bui (kvb2@pitt.edu)
 */
public class FgsdCli extends AbstractAlgorithmCli {

    public static final int CATEGORY_LIMIT = 10;

    protected double structurePrior;
    protected double samplePrior;
    protected int maxDegree;
    protected boolean faithfulnessAssumed;

    protected boolean skipUniqueVarName;
    protected boolean skipCategoryLimit;

    public FgsdCli(String[] args) {
        super(args);
    }

    @Override
    public void printValidationInfos(Formatter fmt) {
        fmt.format("ensure variable names are unique = %s%n", !skipUniqueVarName);
        fmt.format("limit number of categories (%d) = %s%n", CATEGORY_LIMIT, !skipCategoryLimit);
    }

    @Override
    public void printParameterInfos(Formatter fmt) {
        fmt.format("sample prior = %f%n", samplePrior);
        fmt.format("structure prior = %f%n", structurePrior);
        fmt.format("max degree = %d%n", maxDegree);
        fmt.format("faithfulness assumed = %s%n", faithfulnessAssumed);
    }

    @Override
    public Parameters getParameters() {
        Parameters parameters = new Parameters();
        parameters.set(ParamAttrs.SAMPLE_PRIOR, samplePrior);
        parameters.set(ParamAttrs.STRUCTURE_PRIOR, structurePrior);
        parameters.set(ParamAttrs.MAX_DEGREE, maxDegree);
        parameters.set(ParamAttrs.FAITHFULNESS_ASSUMED, faithfulnessAssumed);
        parameters.set(ParamAttrs.VERBOSE, verbose);

        return parameters;
    }

    @Override
    public Algorithm getAlgorithm(IKnowledge knowledge) {
        Fgs fgs = new Fgs(new BdeuScore());
        if (knowledge != null) {
            fgs.setKnowledge(knowledge);
        }

        return fgs;
    }

    @Override
    public DataReader getDataReader(Path dataFile, char delimiter) {
        return new VerticalTabularDiscreteDataReader(dataFile, delimiter);
    }

    @Override
    public List<DataValidation> getDataValidations(DataSet dataSet, Path dirOut, String filePrefix) {
        List<DataValidation> validations = new LinkedList<>();

        String outputDir = dirOut.toString();
        if (!skipUniqueVarName) {
            if (validationOutput) {
                validations.add(new UniqueVariableNames(dataSet, Paths.get(outputDir, filePrefix + "_duplicate_var_name.txt")));
            } else {
                validations.add(new UniqueVariableNames(dataSet));
            }
        }
        if (!skipCategoryLimit) {
            validations.add(new LimitDiscreteCategory(dataSet, CATEGORY_LIMIT));
        }

        return validations;
    }

    @Override
    public void parseRequiredOptions(CommandLine cmd) throws Exception {
    }

    @Override
    public void parseOptionalOptions(CommandLine cmd) throws Exception {
        ParamDescriptions param = ParamDescriptions.instance();

        structurePrior = Args.getDouble(cmd.getOptionValue("structure-prior", String.valueOf(param.get(ParamAttrs.STRUCTURE_PRIOR).getDefaultValue())));
        samplePrior = Args.getDouble(cmd.getOptionValue("sample-prior", String.valueOf(param.get(ParamAttrs.SAMPLE_PRIOR).getDefaultValue())));
        maxDegree = Args.getIntegerMin(cmd.getOptionValue("max-degree", String.valueOf(param.get(ParamAttrs.MAX_DEGREE).getDefaultValue())), -1);
        faithfulnessAssumed = cmd.hasOption("faithfulness-assumed");
        skipUniqueVarName = cmd.hasOption("skip-unique-var-name");
        skipCategoryLimit = cmd.hasOption("skip-category-limit");
    }

    @Override
    public List<Option> getRequiredOptions() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public List<Option> getOptionalOptions() {
        ParamDescriptions param = ParamDescriptions.instance();

        List<Option> options = new LinkedList<>();
        options.add(new Option(null, "structure-prior", true, createDescription(param.get(ParamAttrs.STRUCTURE_PRIOR))));
        options.add(new Option(null, "sample-prior", true, createDescription(param.get(ParamAttrs.SAMPLE_PRIOR))));
        options.add(new Option(null, "max-degree", true, createDescription(param.get(ParamAttrs.MAX_DEGREE))));
        options.add(new Option(null, "faithfulness-assumed", false, createDescription(param.get(ParamAttrs.FAITHFULNESS_ASSUMED))));
        options.add(new Option(null, "skip-unique-var-name", false, "Skip check for unique variable names."));
        options.add(new Option(null, "skip-category-limit", false, "Skip 'limit number of categories' check."));

        return options;
    }

    @Override
    public AlgorithmType getAlgorithmType() {
        return AlgorithmType.FGSD;
    }

}

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Step4 {

    // Load the gold standard keys (ignoring the label here)
    private static Map<String, String> loadGoldStandard(String goldPath) throws Exception {
        Map<String, String> goldMap = new HashMap<>();

        if (goldPath.startsWith("s3:/") && !goldPath.startsWith("s3://")) {
            goldPath = goldPath.replace("s3:/", "s3://");
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(goldPath), conf);
        FSDataInputStream inStream = fs.open(new Path(goldPath));
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

        String line;
        int count = 0;
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split("\t");
            if (tokens.length < 3) continue;
            // Use the key as "word1 word2" exactly as in the file.
            String key = tokens[0].trim() + " " + tokens[1].trim();
            goldMap.put(key, "");
            count++;
        }
        br.close();
        return goldMap;
    }

    // Load the Step3 output into a map
    private static Map<String, String> loadStep3Output(String step3Path) throws Exception {
        Map<String, String> vecMap = new HashMap<>();

        if (step3Path.startsWith("s3:/") && !step3Path.startsWith("s3://")) {
            step3Path = step3Path.replace("s3:/", "s3://");
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(step3Path), conf);
        Path path = new Path(step3Path);

        if (fs.isDirectory(path)) {
            FileStatus[] statuses = fs.listStatus(path);
            for (FileStatus status : statuses) {
                if (!status.isDirectory()) {
                    readStep3File(fs, status.getPath(), vecMap);
                }
            }
        } else {
            readStep3File(fs, path, vecMap);
        }
        return vecMap;
    }

    private static void readStep3File(FileSystem fs, Path filePath, Map<String, String> vecMap) throws Exception {
        FSDataInputStream inStream = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
        String line;
        while ((line = br.readLine()) != null) {
            String lineTrimmed = line.trim();
            if (lineTrimmed.isEmpty()) continue;
            // We assume Step3 output rows are in the format:
            // word1,word2,sim1,sim2,...,sim24,label
            String[] tokens = lineTrimmed.split(",", 3);
            if (tokens.length < 3) continue;
            String word1 = tokens[0].trim();
            String word2 = tokens[1].trim();
            String vectorStr = tokens[2].trim(); // contains 24 similarity values and the label
            String key = word1 + " " + word2;
            vecMap.put(key, vectorStr);
        }
        br.close();
    }
    
    // Helper method to reverse the order of a key "word1 word2" to "word2 word1"
    private static String reverseKey(String key) {
        String[] parts = key.split(" ", 2);
        if (parts.length < 2) return key;
        return parts[1] + " " + parts[0];
    }
    
    // Create the joined CSV file by matching GS keys with Step3 keys (or their reverse)
    private static File createJoinedCSV(String goldPath, String step3Path) throws Exception {
        Map<String, String> goldMap = loadGoldStandard(goldPath);
        Map<String, String> vecMap = loadStep3Output(step3Path);
        File csvFile = File.createTempFile("joined", ".csv");
        BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));

        // Header: word1,word2,sim1,...,sim24,label (27 columns)
        StringBuilder header = new StringBuilder();
        header.append("word1,word2");
        for (int i = 1; i <= 24; i++) {
            header.append(",sim").append(i);
        }
        header.append(",label");
        bw.write(header.toString());
        bw.newLine();

        int joinedCount = 0;
        // For each key in the gold standard, try to find a matching vector in either order.
        for (String key : goldMap.keySet()) {
            String vectorStr = null;
            if (vecMap.containsKey(key)) {
                vectorStr = vecMap.get(key);
            } else {
                String revKey = reverseKey(key);
                if (vecMap.containsKey(revKey)) {
                    vectorStr = vecMap.get(revKey);
                }
            }
            if (vectorStr != null) {
                String[] words = key.split(" ", 2);
                if (words.length < 2) continue;
                String csvLine = words[0] + "," + words[1] + "," + vectorStr;
                bw.write(csvLine);
                bw.newLine();
                joinedCount++;
                System.out.println("[Step4] Joined pair: " + key + " using Step3 label.");
            }
        }
        bw.close();
        return csvFile;
    }

    // Format with fixed precision
    private static String formatDouble(double value) {
        return String.format("%.3f", value);
    }

    // Format with fixed precision and trim trailing zeros
    private static String formatDoubleNoTrail(double value) {
        String formatted = String.format("%.3f", value);
        // Remove trailing zeros and decimal point if needed
        while (formatted.endsWith("0")) {
            formatted = formatted.substring(0, formatted.length() - 1);
        }
        if (formatted.endsWith(".")) {
            formatted = formatted.substring(0, formatted.length() - 1);
        }
        return formatted;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Step4 <filler> <goldStandardFile> <step3OutputPath> <outputResultsFile>");
            System.exit(1);
        }
        
        String goldStandardPath = args[1];
        String step3OutputPath = args[2];
        String outputResults = args[3];
        
        File joinedCSV = createJoinedCSV(goldStandardPath, step3OutputPath);
        
        CSVLoader loader = new CSVLoader();
        loader.setSource(joinedCSV);
        loader.setFieldSeparator(",");
        loader.setNominalAttributes("27");
        Instances data = loader.getDataSet();
        
        if (data.classIndex() == -1) {
            data.setClassIndex(data.numAttributes() - 1);
        }
        
        if (data.numInstances() < 10) {
            System.exit(1);
        }
        
        // Use NaiveBayes instead of J48 for classification.
        Classifier classifier = new NaiveBayes();
        Evaluation evaluation = new Evaluation(data);
        
        // 10-fold cross-validation
        evaluation.crossValidateModel(classifier, data, 10, new Random(1));
        
        // Generate output in the format shown in the image
        StringBuilder results = new StringBuilder();
        
        // Percentage header (e.g., 10%)
        // Percentage header (e.g., 10%)
double pctCorrect = evaluation.pctCorrect();
results.append(pctCorrect).append("%\n\n");
        
        // Table header with alignment
        results.append(String.format("%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s%n",
                "TP Rate", "FP Rate", "Precision", "Recall", "F-Measure", "MCC", "ROC Area", "PRC Area", "Class"));
        
        // True class statistics
        double tpRateTrue = evaluation.truePositiveRate(1);  // Class index 1 is usually "true" or "True"
        double fpRateTrue = evaluation.falsePositiveRate(1);
        double precisionTrue = evaluation.precision(1);
        double recallTrue = evaluation.recall(1);
        double fMeasureTrue = evaluation.fMeasure(1);
        double mccTrue = evaluation.matthewsCorrelationCoefficient(1);
        double rocAreaTrue = evaluation.areaUnderROC(1);
        double prcAreaTrue = evaluation.areaUnderPRC(1);
        
        results.append(String.format("%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s%n",
                formatDoubleNoTrail(tpRateTrue), formatDoubleNoTrail(fpRateTrue), 
                formatDoubleNoTrail(precisionTrue), formatDoubleNoTrail(recallTrue), 
                formatDoubleNoTrail(fMeasureTrue), formatDoubleNoTrail(mccTrue), 
                formatDoubleNoTrail(rocAreaTrue), formatDoubleNoTrail(prcAreaTrue), "true"));
        
        // False class statistics
        double tpRateFalse = evaluation.truePositiveRate(0);  // Class index 0 is usually "false" or "False"
        double fpRateFalse = evaluation.falsePositiveRate(0);
        double precisionFalse = evaluation.precision(0);
        double recallFalse = evaluation.recall(0);
        double fMeasureFalse = evaluation.fMeasure(0);
        double mccFalse = evaluation.matthewsCorrelationCoefficient(0);
        double rocAreaFalse = evaluation.areaUnderROC(0);
        double prcAreaFalse = evaluation.areaUnderPRC(0);
        
        results.append(String.format("%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s%n",
                formatDoubleNoTrail(tpRateFalse), formatDoubleNoTrail(fpRateFalse), 
                formatDoubleNoTrail(precisionFalse), formatDoubleNoTrail(recallFalse), 
                formatDoubleNoTrail(fMeasureFalse), formatDoubleNoTrail(mccFalse), 
                formatDoubleNoTrail(rocAreaFalse), formatDoubleNoTrail(prcAreaFalse), "false"));
        
        // Weighted average row
        double weightedTpRate = evaluation.weightedTruePositiveRate();
        double weightedFpRate = evaluation.weightedFalsePositiveRate();
        double weightedPrecision = evaluation.weightedPrecision();
        double weightedRecall = evaluation.weightedRecall();
        double weightedFMeasure = evaluation.weightedFMeasure();
        double weightedMcc = evaluation.weightedMatthewsCorrelation();
        double weightedRocArea = evaluation.weightedAreaUnderROC();
        double weightedPrcArea = evaluation.weightedAreaUnderPRC();
        
        results.append(String.format("%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s%n",
                formatDoubleNoTrail(weightedTpRate), formatDoubleNoTrail(weightedFpRate), 
                formatDoubleNoTrail(weightedPrecision), formatDoubleNoTrail(weightedRecall), 
                formatDoubleNoTrail(weightedFMeasure), formatDoubleNoTrail(weightedMcc), 
                formatDoubleNoTrail(weightedRocArea), formatDoubleNoTrail(weightedPrcArea)));
        
        // Confusion Matrix
        results.append("\nConfusion Matrix\n======\n\n");
        
        // Calculate confusion matrix values
        int tp = (int) evaluation.numTruePositives(1);  // true positives for class "true"
        int fp = (int) evaluation.numFalsePositives(1); // false positives for class "true"
        int fn = (int) evaluation.numFalseNegatives(1); // false negatives for class "true"
        int tn = (int) evaluation.numTrueNegatives(1);  // true negatives for class "true"
        
        // Format confusion matrix like in the image
        results.append(String.format("    a    b    <-- classified as%n"));
        results.append(String.format(" %4d %4d |    a = true%n", tp, fn));
        results.append(String.format(" %4d %4d |    b = false%n", fp, tn));
        
        // Write results to the output file
        Configuration conf = new Configuration();
        if (outputResults.startsWith("s3://")) {
            FileSystem fs = FileSystem.get(new URI(outputResults), conf);
            FSDataOutputStream out = fs.create(new Path(outputResults));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
            writer.write(results.toString());
            writer.close();
        } else {
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputResults)));
            writer.write(results.toString());
            writer.close();
        }
        
        // Also print to console for debugging
        System.out.println(results.toString());
    }
}
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step3 {

    public static class FuzzyJoinMapper extends Mapper<Text, Text, Text, Text> {
        private GS gs;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            gs = GS.getInstance();
            System.out.println("DEBUG: FuzzyJoinMapper setup complete. GS instance loaded.");
        }
        
        @Override
        protected void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {
            String head = key.toString().trim();
            String vectorStr = value.toString().trim();
            Set<String> paired = gs.getSetFromPairs(head);
            //System.out.println("DEBUG: For head '" + head + "', paired words: " + paired);
            if (paired == null) {
                //System.out.println("DEBUG: No paired words for head '" + head + "'. Skipping.");
                return;
            }
            for (String other : paired) {
                String compositeKey;
                if (head.compareTo(other) < 0) {
                    compositeKey = head + " " + other;
                } else if (head.compareTo(other) > 0) {
                    compositeKey = other + " " + head;
                } else {
                    //System.out.println("DEBUG: Skipping same word pair for: " + head);
                    continue;
                }
                String outVal = head + "|" + vectorStr;
                System.out.println("DEBUG: Emitting composite key: '" + compositeKey + "', value: '" + outVal + "'");
                context.write(new Text(compositeKey), new Text(outVal));
            }
        }
    }
    
    public static class FuzzyJoinReducer extends Reducer<Text, Text, Text, Text> {
        private GS gs;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            gs = GS.getInstance();
           // System.out.println("DEBUG: FuzzyJoinReducer setup complete. GS instance loaded.");
        }
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           // System.out.println("DEBUG: Reducer processing composite key: '" + key.toString() + "'");
            String word1 = null;
            String vecStr1 = null;
            String word2 = null;
            String vecStr2 = null;
            String[] words=key.toString().split(" ");
            word1=words[0];
            word2=words[1];
            
            for (Text val : values) {
                String token = val.toString();
                System.out.println("DEBUG: Reducer received token: '" + token + "'");
                String[] parts = token.split("\\|", 2);
                if (parts.length < 2) {
                    System.out.println("DEBUG: Token format invalid: '" + token + "'. Skipping.");
                    continue;
                }
                String w = parts[0];
                String v = parts[1];
                if (w.equals(word1)) {
                    vecStr1 = v;
                } else if (w.equals(word2)) {
                    vecStr2 = v;
                } else {
                    System.out.println("DEBUG: Duplicate word encountered: " + w);
                }
            }
            if (word1 == null || word2 == null) {
                //System.out.println("DEBUG: Not enough data for composite key '" + key.toString() + "'. Skipping.");
                return;
            }
            System.out.println("DEBUG: Composite key '" + key.toString() + "' corresponds to words: " + word1 + " and " + word2);
            if (vecStr1 == null || vecStr2 == null) {
               // System.out.println("DEBUG: One of the vector strings is null for pair (" + word1 + ", " + word2 + "). Skipping.");
                return;
            }
            double[][] vec1 = parseVectors(vecStr1);
            double[][] vec2 = parseVectors(vecStr2);
            if (vec1 == null || vec2 == null) {
                //System.out.println("DEBUG: Error parsing vectors for composite key '" + key.toString() + "'. Skipping.");
                return;
            }
            //System.out.println("DEBUG: Parsed vector for " + word1 + ": " + Arrays.deepToString(vec1));
            //System.out.println("DEBUG: Parsed vector for " + word2 + ": " + Arrays.deepToString(vec2));
            
            double[] sims = new double[24];
            int idx = 0;
            for (int m = 0; m < 4; m++) {
                double[] v1 = vec1[m];
                double[] v2 = vec2[m];
                // Calculate Manhattan distance and convert to similarity
                double manhattanDist = computeManhattanDistance(v1, v2);
            //sims[idx++] = 1.0 / (1.0 + manhattanDist);
                sims[idx++] = manhattanDist;

                
                // Calculate Euclidean distance and convert to similarity
                double euclideanDist = computeEuclideanDistance(v1, v2);
                //sims[idx++] = 1.0 / (1.0 + euclideanDist);
                sims[idx++] = euclideanDist;


                // Calculate Cosine similarity directly
                sims[idx++] = computeCosineSimilarity(v1, v2);
                
                // Calculate Jaccard similarity
                sims[idx++] = computeJaccardSimilarity(v1, v2);
                
                // Calculate Dice similarity
                sims[idx++] = computeDiceSimilarity(v1, v2);
                
                // Calculate Jensen-Shannon divergence and convert to similarity
                double jsDivergence = computeJSDivergence(v1, v2);
                //sims[idx++] = 1.0 / (1.0 + jsDivergence);
                sims[idx++] =jsDivergence;

            }
            //System.out.println("DEBUG: Computed similarity vector for pair (" + word1 + ", " + word2 + "): " + Arrays.toString(sims));
            
            StringBuilder sb = new StringBuilder();
            sb.append(word1).append(",").append(word2);
            for (double sim : sims) {
                sb.append(",").append(sim);
            }
            String label = gs.getLabel(word1, word2);
            if (label == null) {
                label = "unknown";
            }
            sb.append(",").append(label);
            //System.out.println("DEBUG: Emitting CSV row: " + sb.toString());
            context.write(new Text(), new Text(sb.toString()));
        }
        
        private double[][] parseVectors(String vectorStr) {
            
            String[] groups = vectorStr.split(";");
            if (groups.length != 4) {
                //System.out.println("DEBUG: Unexpected vector format: " + vectorStr);
                return null;
            }
            double[][] result = new double[4][];
            for (int i = 0; i < 4; i++) {
                String[] tokens = groups[i].split(",");
                result[i] = new double[tokens.length];
                for (int j = 0; j < tokens.length; j++) {
                    try {
                        result[i][j] = Double.parseDouble(tokens[j].trim());
                    } catch (NumberFormatException e) {
                        //System.out.println("DEBUG: NumberFormatException for token: " + tokens[j] + " in group " + i);
                        result[i][j] = 0.0;
                    }
                }
            }
            return result;
        }
        
        // Formula (9): Manhattan Distance
        private double computeManhattanDistance(double[] v1, double[] v2) {
            double sum = 0.0;
            for (int i = 0; i < v1.length; i++) {
                sum += Math.abs(v1[i] - v2[i]);
            }
            return sum;
        }
        
        // Formula (10): Euclidean Distance
        private double computeEuclideanDistance(double[] v1, double[] v2) {
            double sum = 0.0;
            for (int i = 0; i < v1.length; i++) {
                sum += Math.pow(v1[i] - v2[i], 2);
            }
            return Math.sqrt(sum);
        }
        
        // Formula (11): Cosine Similarity
        private double computeCosineSimilarity(double[] v1, double[] v2) {
            double dot = 0.0, norm1 = 0.0, norm2 = 0.0;
            for (int i = 0; i < v1.length; i++) {
                dot += v1[i] * v2[i];
                norm1 += v1[i] * v1[i];
                norm2 += v2[i] * v2[i];
            }
            if (norm1 == 0 || norm2 == 0) return 0.0;
            return dot / (Math.sqrt(norm1) * Math.sqrt(norm2));
        }
        
        // Formula (13): Jaccard Similarity
        private double computeJaccardSimilarity(double[] v1, double[] v2) {
            double minSum = 0.0, maxSum = 0.0;
            for (int i = 0; i < v1.length; i++) {
                minSum += Math.min(v1[i], v2[i]);
                maxSum += Math.max(v1[i], v2[i]);
            }
            if (maxSum == 0) return 0.0;
            return minSum / maxSum;
        }
        
        // Formula (15): Dice Similarity
        private double computeDiceSimilarity(double[] v1, double[] v2) {
            double minSum = 0.0, sum = 0.0;
            for (int i = 0; i < v1.length; i++) {
                minSum += Math.min(v1[i], v2[i]);
                sum += v1[i] + v2[i];
            }
            if (sum == 0) return 0.0;
            return (2 * minSum) / sum;
        }
        
        private double computeJSDivergence(double[] v1, double[] v2) {
            double sum1 = 0.0, sum2 = 0.0;
            for (double d : v1) sum1 += d;
            for (double d : v2) sum2 += d;
            double[] p1 = new double[v1.length];
            double[] p2 = new double[v2.length];
            for (int i = 0; i < v1.length; i++) {
                p1[i] = (sum1 > 0) ? v1[i] / sum1 : 0.0;
                p2[i] = (sum2 > 0) ? v2[i] / sum2 : 0.0;
            }
            double kl1 = 0.0, kl2 = 0.0;
            for (int i = 0; i < p1.length; i++) {
                double m = (p1[i] + p2[i]) / 2.0;
                if (p1[i] > 0 && m > 0)
                    kl1 += p1[i] * Math.log(p1[i] / m);
                if (p2[i] > 0 && m > 0)
                    kl2 += p2[i] * Math.log(p2[i] / m);
            }
            return 0.5 * (kl1 + kl2);
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Step3 <filler> <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3: Fuzzy Join for 24-Dim Similarity Vectors");
        job.setJarByClass(Step3.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.setMapperClass(FuzzyJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(FuzzyJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(1);
        
        System.out.println("[Step3] Submitting job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
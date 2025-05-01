import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class Step2 {
 
    public static class VectorMapper extends Mapper<Text, MyHashMapWritable, Text, Text> {
        private GS gs;
        private String[] topFeatures;
        private Map<String, Integer> topFeatureCounts = new HashMap<>();
        private long COUNT_L = 0;
        private long COUNT_F = 0;
 
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            gs = GS.getInstance();
            Configuration conf = context.getConfiguration();
            String featuresPathStr ="s3://ass3emb3151/output/SortFeatures/" ;
 
            /*  Fix S3 path if necessary.
            if (featuresPathStr != null && featuresPathStr.startsWith("s3:/") && !featuresPathStr.startsWith("s3://")) {
                featuresPathStr = featuresPathStr.replace("s3:/", "s3://");
            }*/
 
            List<String> featureList = new ArrayList<>();
 
            if (featuresPathStr != null) {
                Path featuresPath = new Path(featuresPathStr);
                FileSystem fs = featuresPath.getFileSystem(conf);
                Path fileToRead = featuresPath;
                if (fs.isDirectory(featuresPath)) {
                    FileStatus[] statuses = fs.listStatus(featuresPath);
                    boolean fileFound = false;
                    for (FileStatus status : statuses) {
                        String name = status.getPath().getName();
                        if (!name.startsWith("_") && !name.startsWith(".")) {
                            fileToRead = status.getPath();
                            fileFound = true;
                            break;
                        }
                    }
                    if (!fileFound) {
                        throw new IOException("No valid file found in directory: " + featuresPath.toString());
                    }
                }
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileToRead)));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    if (tokens.length >= 2) {
                        String feat = tokens[0].trim();
                        // Skip the special keys
                        if (feat.equals("counter_of_this_word") || feat.equals("")) {
                            continue;
                        }
                        if(feat.equals("*")){
                            COUNT_L=Long.parseLong(tokens[1].trim());
                            System.out.println("found count l");
                            System.out.println(COUNT_L);
                            continue;
                        }
                        if(feat.equals("**")){
                            COUNT_F=Long.parseLong(tokens[1].trim());
                            System.out.println("found count f");
                            System.out.println(COUNT_F);
                            continue;
                        }
                        try {
                            int count = Integer.parseInt(tokens[1].trim());
                            featureList.add(feat);
                            System.out.println(feat);
                            topFeatureCounts.put(feat, count);
                        } catch (NumberFormatException e) {
                            // Skip improperly formatted lines
                        }
                    }
                }
                br.close();
                topFeatures = featureList.toArray(new String[featureList.size()]);
                System.out.println("[step2] has   "+featureList.size()+"    features");

            }
        }
 
        @Override
        protected void map(Text key, MyHashMapWritable value, Context context)
                throws IOException, InterruptedException {
            String head = key.toString();
 
            // Process special keys for global totals.
            if (head.equals("*")) {
                COUNT_L = value.get("COUNT_L");
                System.out.println(COUNT_L);

                return;
            }
            if (head.equals("**")) {
                COUNT_F = value.get("COUNT_F");
                System.out.println(COUNT_F);


                return;
            }
 
            // Process only headwords that are part of the GS.
            if (!gs.ContainInWords(head)) {
                return;
            }
 
            // Get the stripe from step1 output.
            Map<String, Long> stripe = value.getMap();
            // Instead of summing over the stripe, use the provided count for the head word.
            long countHead = stripe.getOrDefault("counter_of_this_word", 0L);
            if (countHead == 0) return;
 
            int N = topFeatures.length;
            double[] rawVec = new double[N];
            double[] mleVec = new double[N];
            double[] pmiVec = new double[N];
            double[] ttestVec = new double[N];
 
            for (int i = 0; i < N; i++) {
                String feat = topFeatures[i];
                // Get count(F=f, L=l)
                long countLF = stripe.getOrDefault(feat, 0L);
                rawVec[i] = countLF;
                mleVec[i] = (double) countLF / countHead;
 
                // Get count(F = f) from the top features count
                int featCount = topFeatureCounts.getOrDefault(feat, 0);
                if (COUNT_L > 0 && COUNT_F > 0) {
                    double p_lf = (double) countLF / COUNT_L;
                    //double p_l_if_f = (double) countLF / countHead;
                    double p_l= (double) countHead / COUNT_L;
                    double p_f = featCount / COUNT_F;
                    if (countLF > 0 && p_l > 0 && p_f > 0) {
                        pmiVec[i] = (double)((Math.log(p_lf / (p_l * p_f))) / (Math.log(2)));
                    } else {
                        /*if(countLF==0){
                            pmiVec[i] = 1.0;
                        }
                        if(p_l<0.000000001){
                            pmiVec[i] = 2.0;
                        }
                        if(p_f==0){
                            pmiVec[i] = 3.0;
                        }*/
                       pmiVec[i] = countHead; 
                    }
                    double expected =(double) (p_l * p_f);
                    if (expected > 0) {
                        ttestVec[i] =(double)( (p_lf - expected) / Math.sqrt(expected));
                    } else {
                        ttestVec[i] = 0.0;
                    }
                } else {
                    pmiVec[i] = 0.0;
                        ttestVec[i] = 0.0;
                    //mleVec[i] = 0.0;
                   
                }
            }
 
            String rawStr = arrayToString(rawVec);
            String mleStr = arrayToString(mleVec);
            String pmiStr = arrayToString(pmiVec);
            String ttestStr = arrayToString(ttestVec);
            String outputValue = rawStr + ";" + mleStr + ";" + pmiStr + ";" + ttestStr;
 
            context.write(new Text(head), new Text(outputValue));
        }
 
        private String arrayToString(double[] arr) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < arr.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(arr[i]);
            }
            return sb.toString();
        }
    }
 
    public static class IdentityReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
                break;
            }
        }
    }
 
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Step2 <filler> <input path> <sorted features path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("sorted.features.path", args[2]);
        Job job = Job.getInstance(conf, "Step2: Compute Association Measures");
        job.setJarByClass(Step2.class);
 
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
 
        job.setMapperClass(VectorMapper.class);
        job.setReducerClass(IdentityReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
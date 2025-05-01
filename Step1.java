import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Step1 {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, MyHashMapWritable> {
        private Text outKey = new Text();
        // Global counters (using long to avoid integer overflow)
        private long count_L = 0;
        private long count_F = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            //System.out.println("[DEBUG] Processing line: " + line);

            // Split the line by tabs
            String[] fields = line.split("\t", -1);
            if (fields.length < 3) {
                //System.out.println("[DEBUG] Skipping line, not enough fields: " + fields.length);
                return;
            }

            // The first field is the head word (we'll still parse it along with others)
            String headWord = fields[0].trim().toLowerCase();
            //System.out.println("[DEBUG] Head word: " + headWord);

            // Second field contains the dependency structure
            String dependencyStr = fields[1].trim();
            //System.out.println("[DEBUG] Dependency structure: " + dependencyStr);
            
            // Parse count - it should be in a field after the dependency structure
            int count = 1;
            try {
                // Find the first numeric field after the dependency structure
                for (int i = 2; i < fields.length; i++) {
                    if (fields[i].trim().matches("\\d+")) {
                        count = Integer.parseInt(fields[i].trim());
                        //System.out.println("[DEBUG] Found count: " + count + " at position " + i);
                        break;
                    }
                }
            } catch(NumberFormatException e) {
                //System.out.println("[DEBUG] Error parsing count, using default: 1");
                count = 1;
            }

            // Split the dependency string into separate word structures
            String[] tokens = dependencyStr.split("\\s+");
            //System.out.println("[DEBUG] Found " + tokens.length + " tokens in dependency structure");
            
            // Create arrays to store word information
            String[] words = new String[tokens.length];
            String[] roles = new String[tokens.length];
            int[] pointers = new int[tokens.length];
            //MyHashMapWritable[] strips =new MyHashMapWritable[tokens.length];

            
            // First pass: Parse words, roles and pointers
            for (int i = 0; i < tokens.length; i++) {
                String token = tokens[i].trim();
                if (token.isEmpty()) continue;
                
                String[] parts = token.split("/");
                if (parts.length < 4) {
                    //System.out.println("[DEBUG] Token " + (i+1) + " has insufficient parts: " + parts.length);
                    continue;
                }
                
                words[i] = parts[0].trim().toLowerCase();
                roles[i] = parts[2].trim().toLowerCase();
                
                // Parse the pointer (which word this points to)
                try {
                    pointers[i] = Integer.parseInt(parts[parts.length - 1]);
                    //System.out.println("[DEBUG] Token " + (i+1) + " (" + words[i] + ") points to: " + pointers[i]);
                } catch (NumberFormatException e) {
                    //System.out.println("[DEBUG] Error parsing pointer for token " + (i+1));
                    pointers[i] = 0; // Default to 0 (no pointer)
                }
                
                //System.out.println("[DEBUG] Position " + (i+1) + ": word=" + words[i] + ", role=" + roles[i] + ", points to=" + pointers[i]);
            }
            
            // Second pass: Create word relationships based on pointers
            for (int i = 0; i < tokens.length; i++) {
                // Skip if no word at this position
                if (words[i] == null) continue;
                
                // Create hashmap for this word
                MyHashMapWritable stripe = new MyHashMapWritable();
                stripe.put("counter_of_this_word", (long) count);
                count_L+=(long)count;
                
                
                // Check if this word points to another word
                /*if (pointers[i] > 0 && pointers[i] <= tokens.length) {
                    int targetIdx = pointers[i] - 1; // Convert to 0-based index
                    
                    // If valid target and not pointing to itself
                    if (targetIdx != i && words[targetIdx] != null) {
                        // Add the target word with its role as a feature
                        String feature = words[targetIdx] + "-" + roles[targetIdx];
                        stripe.put(feature, (long) count);
                        count_F++;
                        System.out.println("[DEBUG] Word " + words[i] + " has feature: " + feature);
                    }
                }*/
                
                // Find any words that point to this word
                for (int j = 0; j < tokens.length; j++) {
                    if (j != i && pointers[j] == i + 1) { // Convert this position to 1-based
                        if (words[j] != null) {
                            // Add the dependent word with its role as a feature
                            String feature = words[j] + "-" + roles[j];
                            stripe.put(feature, (long) count);
                            count_F+=(long)count;
                            //System.out.println("[DEBUG] Word " + words[i] + " has feature: " + feature);
                        }
                    }
                }
                
                // Emit record for this word
                outKey.set(words[i]);
                context.write(outKey, stripe);
                //System.out.println("[DEBUG] Emitted record for: " + words[i]);
                
                //count_L += count;
            }
            
            //System.out.println("[DEBUG] Finished processing line\n");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit global totals using special keys
            Text totalKey = new Text("*");
            MyHashMapWritable totalStripe = new MyHashMapWritable();
            totalStripe.put("COUNT_L", count_L);
            context.write(totalKey, totalStripe);
            System.out.println("[DEBUG] Emitted COUNT_L: " + count_L);

            Text featureKey = new Text("**");
            MyHashMapWritable featureStripe = new MyHashMapWritable();
            featureStripe.put("COUNT_F", count_F);
            context.write(featureKey, featureStripe);
            System.out.println("[DEBUG] Emitted COUNT_F: " + count_F);
        }
    }

    public static class MyReducer extends Reducer<Text, MyHashMapWritable, Text, MyHashMapWritable> {
        @Override
        protected void reduce(Text key, Iterable<MyHashMapWritable> values, Context context)
                throws IOException, InterruptedException {
            MyHashMapWritable merged = new MyHashMapWritable();
            for (MyHashMapWritable stripe : values) {
                merged.merge(stripe);
            }
            context.write(key, merged);
            //System.out.println("[DEBUG-REDUCER] Reduced key: " + key.toString() + " with features: " + merged);
        }
    }

    public static class MyPartitioner extends Partitioner<Text, MyHashMapWritable> {
        @Override
        public int getPartition(Text key, MyHashMapWritable value, int numPartitions) {
            String k = key.toString();
            if (k.equals("*") || k.equals("**")) {
                return 0;
            } else {
                return (k.hashCode() & Integer.MAX_VALUE) % numPartitions;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Step1 <filler> <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1: Build word relationship maps from syntactic n-grams");
        job.setJarByClass(Step1.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyHashMapWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setPartitionerClass(MyPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.out.println("[DEBUG-MAIN] Starting job with input: " + args[1] + ", output: " + args[2]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
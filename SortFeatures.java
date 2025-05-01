import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortFeatures {
    // Mapper: Swap <feature, count> to <count, feature>
    public static class SwapMapper extends Mapper<Text, LongWritable, LongWritable, Text> {
        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();
        
        @Override
        protected void map(Text key, LongWritable value, Context context)
        throws IOException, InterruptedException {
            String keyStr = key.toString();
            // Skip special keys: "*", "**", and "count"
            if (keyStr.equals("counter_of_this_word")) {
                //System.out.println("[SwapMapper] Skipping special key: " + keyStr);
                context.getCounter("SwapMapper", "SkippedSpecialKeys").increment(1);
                return;
            }
    // Negate the value for descending sort
    outKey.set(-value.get());  // Negative for descending sort
    outValue.set(key);
    //System.out.println("[SwapMapper] Emitting (" + outKey + ", " + outValue + ")");
    context.write(outKey, outValue);
}
    }

    // Reducer: With a single reducer (global sorting), skip the top 100 records and output the next 1000.
    public static class TopFeaturesReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        private int recordCount = 0;
        
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Copy the key value locally.
            long currentCount = -key.get();  // Convert back to positive
            for (Text feature : values) {
                recordCount++;
                if(feature.toString().equals("*")){
                    context.write(feature, new LongWritable(currentCount));
                }
                if(feature.toString().equals("**")){
                    context.write(feature, new LongWritable(currentCount));
                }
                //System.out.println("[TopFeaturesReducer] Processing record #" + recordCount 
                            //      + " for feature: " + feature + " with count: " + currentCount);
                // Omit the top 100 most frequent features.
                if (recordCount <= 100) {
                    System.out.println("[TopFeaturesReducer] Skipping top feature #" + recordCount);
                    continue;
                }
                // Stop after outputting 1000 features (records 101 to 1100)
                if (recordCount > 1100) {
                    System.out.println("[TopFeaturesReducer] Reached 1100 records. Terminating reducer.");
                    return;
                }
                context.write(feature, new LongWritable(currentCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SortFeatures <filler> <input path> <output path>");
            System.exit(-1);
        }
        
        System.out.println("[SortFeatures] Starting job with input: " + args[1] + " and output: " + args[2]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SortFeatures: Select Top 1000 (omitting top 100) Most Frequent Features");
        job.setJarByClass(SortFeatures.class);
        
        // Run with a single reducer to ensure global sorting.
        job.setNumReduceTasks(1);
        
        // Input format is a SequenceFile (<feature, count>) where count is now LongWritable.
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // Output as text.
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapperClass(SwapMapper.class);
        job.setReducerClass(TopFeaturesReducer.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.out.println("[SortFeatures] Submitting job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

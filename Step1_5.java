import java.io.IOException;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
 
public class Step1_5 {
 
    public static class FeatureCountMapper extends Mapper<Text, MyHashMapWritable, Text, LongWritable> {
        private Text featureKey = new Text();
        private LongWritable countValue = new LongWritable();
 
        @Override
        protected void map(Text key, MyHashMapWritable value, Context context)
                throws IOException, InterruptedException {
            String head = key.toString();
            // Handle special keys for global totals.
            if (head.equals("*")) {
                // COUNT_L is stored as a Long in MyHashMapWritable.
                Long countL = value.get("COUNT_L");
                if (countL != null) {
                    countValue.set(countL);
                    //System.out.println("[Step1_5-Mapper] Special key '*' detected. Emitting COUNT_L: " + countL);
                    context.write(key, countValue);
                }
                return;
            }
            if (head.equals("**")) {
                Long countF = value.get("COUNT_F");
                if (countF != null) {
                    countValue.set(countF);
                    //System.out.println("[Step1_5-Mapper] Special key '**' detected. Emitting COUNT_F: " + countF);
                    context.write(key, countValue);
                }
                return;
            }
 
            // For normal keys, get the stripe as Map<String, Long>
            Map<String, Long> stripe = value.getMap();
            for (Map.Entry<String, Long> entry : stripe.entrySet()) {
                String feature = entry.getKey();
                long count = entry.getValue();
                featureKey.set(feature);
                countValue.set(count);
                //System.out.println("[Step1_5-Mapper] Emitting feature: " + feature + " with count: " + count);
                context.write(featureKey, countValue);
            }
        }
    }
 
    public static class FeatureCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable sumWritable = new LongWritable();
 
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            sumWritable.set(sum);
            //System.out.println("[Step1_5-Reducer] Key: " + key + " aggregated count: " + sum);
            context.write(key, sumWritable);
        }
    }
 
    public static class FeatureCountPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            int partition = (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
            //System.out.println("[Step1_5-Partitioner] Key: " + key + " assigned to partition: " + partition);
            return partition;
        }
    }
 
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Step1_5 <filler> <input path> <output path>");
            System.exit(-1);
        }
        System.out.println("[Step1_5] Starting job with input: " + args[1] + " and output: " + args[2]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1_5: Count occurrences for each feature");
        job.setJarByClass(Step1_5.class);
 
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
 
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
 
        job.setMapperClass(FeatureCountMapper.class);
        job.setCombinerClass(FeatureCountReducer.class);
        job.setReducerClass(FeatureCountReducer.class);
 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
 
        job.setPartitionerClass(FeatureCountPartitioner.class);
 
        System.out.println("[Step1_5] Submitting job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

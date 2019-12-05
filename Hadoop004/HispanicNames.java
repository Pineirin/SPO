import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HispanicNames {

	public static class HispanicMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			// The output "state"
			String state = line[3];
			word.set(state);

			// The output "product"
			String product = line[2];
			if(product.equals("HISPANIC")){
				// Record the output in the Context object
				context.write(word, one);
			}
		}
	}

	public static class HispanicReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {		
		// Create the job specification object
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Hispanic count");
		job.setJarByClass(HispanicNames.class);

		// Setup input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Set the Mapper and Reducer classes
		job.setMapperClass(HispanicMapper.class);
		job.setReducerClass(HispanicReducer.class);

		// Specify the type of output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Wait for the job to finish before terminating
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

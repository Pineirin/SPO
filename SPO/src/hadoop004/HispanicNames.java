package hadoop004;

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
	
		/**
		 * The "Mapper" function. It receives a line of input from the file, 
		 * extracts "state" and "Product" from it, which becomes
		 * the output only when the product is a mortage.
		 * The output key is "state" and the output value is 
		 * "1".
		 * @param key - Input key - The line offset in the file - ignored.
		 * @param value - Input Value - This is the line itself.
		 * @param context - Provides access to the OutputCollector and Reporter.
		 * @throws IOException
		 * @throws InterruptedException 
		 */
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
		/**
		 * The "Reducer" function. Iterates through all states to find the number of mortgages. 
		 * The output key is the "state" and  
		 * the value is the "number of mortgages" for that state.
		 * @param key - Input key - Name of the region
		 * @param values - Input Value - Iterator over quake magnitudes for region
		 * @param context - Used for collecting output
		 * @throws IOException
		 * @throws InterruptedException 
		 */
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
		job.setJarByClass(Mortgage.class);

		// Setup input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Set the Mapper and Reducer classes
		job.setMapperClass(MortgageMapper.class);
		job.setReducerClass(MortgageReducer.class);

		// Specify the type of output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Wait for the job to finish before terminating
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WhiteHateCrimes {

	public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			// The output "year"
			String year = line[1];
			word.set(year);
			
			String numberWhite = line[8];
			if(numberWhite > 0){
				// Record the output in the Context object
				context.write(word, one);
			}
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			String year = line[0];
			word.set(year);

			String eth = line[2];
			if(eth.equals("WHITE NON HISPANIC")){
				// Record the output in the Context object
				context.write(word, one);
			}
		}
	}

	public static class NamesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		Job job = Job.getInstance(conf, "Ethnic variaty");
		job.setJarByClass(NamesSalary.class);

		// Setup input and output paths
		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, Mapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(NamesReducer.class);

		// Specify the type of output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Wait for the job to finish before terminating
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

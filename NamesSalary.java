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

public class HateCrimes {

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		
		private Text nWhiteCrimes = new Text();
		private Text year = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			// The output "year"
			String currentYear = line[1];
			year.set(currentYear);
			
			String white = line[8];
			int whiteNumber = 0;
			try {
                		whiteNumber = Integer.valueOf(white);
        		} catch(NumberFormatException e) {
        		}
			if(whiteNumber > 0){
				// Record the output in the Context object
				nWhiteCrimes.set(String.valueOf(whiteNumber));
				context.write(year, nWhiteCrimes);
			}
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		
		private Text newBorns = new Text();
		private Text year = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			String currentYear = line[0];
			year.set(currentYear);

			String eth = line[2];
			if(eth.equals("WHITE NON HISPANIC")){
				// Record the output in the Context object
				newBorns.set(eth);
				context.write(year, newBorns);
			}
		}
	}

	public static class HateReducer extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			String[] val = new String[2];
			int i = 0;
			for (Text n : values) {
				val[i] = n.toString();
				i++;
			}
			context.write(key, new Text(val[1] + " " + val[0]));
		}
	}

	public static void main(String[] args) throws Exception {		
		// Create the job specification object
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Ethnic variaty");
		job.setJarByClass(HateCrimes.class);

		// Setup input and output paths
		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, Mapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(HateReducer.class);

		// Specify the type of output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Wait for the job to finish before terminating
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

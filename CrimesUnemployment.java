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

public class CrimesUnemployment {

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {
		
		private Text crimes = new Text();
		private Text year = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			String currentYear = line[0];
			if (currentYear.equals("2011") || currentYear.equals("2012") || currentYear.equals("2013")) {
				year.set(currentYear);
        crimes.set("Violent crimes rate: " + line[1] + "%");
			  context.write(year, crimes);
			  }
		}
	}
	
	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
		
		private Text crimes = new Text();
		private Text year = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			String currentYear = line[0];
			if (currentYear.equals("2011") || currentYear.equals("2012") || currentYear.equals("2013")) {
				year.set(currentYear);
        crimes.set("Non violent crimes rate: " + line[1] + "%");
			  context.write(year, crimes);
			  }
		}
	}
	
	public static class Mapper3 extends Mapper<Object, Text, Text, Text> {
		
		private Text unemployment = new Text();
		private Text year = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");

			String currentYear = line[0];
			if ((currentYear.contains("2011") || currentYear.contains("2012") || currentYear.contains("2013")) && !currentYear.contains("Fiscal")) {
				
				year.set(currentYear.split("-")[1]);
        unemployment.set(line[6]);
			  context.write(year, unemployment);
			  }
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] val = new String[3];
			int i = 0;
			for (Text n : values) {
				val[i] = n.toString();
				i++;
			}
			String VC = "";
			String NVC = "";
			String UR = "";
			for (int j = 0; j<val.length; j++){
				if (val[j].contains("Violent crimes")){
					VC = val[j];
				} else if (val[j].contains("Non violent crimes")){
					NVC = val[j];
				}
				else {
					UR = val[j];
				}
			}
			context.write(key, new Text("Unemployment rate: " + UR + " " + NVC + " " + VC));
		}
	}

	public static void main(String[] args) throws Exception {		
		// Create the job specification object
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Crimes Unemployment");
		job.setJarByClass(CrimesUnemployment.class);

		// Setup input and output paths
		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, Mapper2.class);
		MultipleInputs.addInputPath(job,new Path(args[2]), TextInputFormat.class, Mapper3.class);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.setReducerClass(Reducer1.class);

		// Specify the type of output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Wait for the job to finish before terminating
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

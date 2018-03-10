import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class PageRank {
	
	private final static int RUNS = 1;
	
	// args example = ["/input", "/output", "/input/pagerank_data.txt", "0.85"]
	public static void main(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.out.println("Invalid arguments, expected 4 (inputpath, outputpath, datapath).");
			System.exit(1);
		}
		
		FileSystem fs = FileSystem.get(new Configuration());
			
		// Deleting the output folder if needed
		Path outputPath = new Path(args[1]);
		if (fs.exists(outputPath))
		{
			System.out.println("Deleting /output..");
			fs.delete(outputPath, true);
		}
		
		// Step 1
		boolean success = step1(args[0], args[1] + "/ranks0");
		
		// Step 2
		for (int i = 0; i < RUNS; i++) 
		{
			System.out.println("Run #" + (i + 1));
			success = success && step2(args[1] + "/ranks" + i, args[1] + "/ranks" + (i + 1));
		}
		
		// Step 3
		success = success && step3(args[1] + "/ranks" + RUNS, args[1] + "/ranking", args[2]);
		
		System.exit(success ? 0 : 1);
	}
	
	private static boolean step1(String input, String output) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

		System.out.println("Step 1..");
		Job job = Job.getInstance(conf, "Step 1");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(Step1Mapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(Step1Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
	private static boolean step2(String input, String output) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		
		System.out.println("Step 2..");
		Job job = Job.getInstance(conf, "Step 2");
		job.setJarByClass(PageRank.class);
		
		job.setMapperClass(Step2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setReducerClass(Step2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}

	private static boolean step3(String input, String output, String urlsPath) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
		conf.set("urls_path", urlsPath);
		
		System.out.println("Step 3..");
		Job job = Job.getInstance(conf, "Step 3");
		job.setJarByClass(PageRank.class);
		job.setSortComparatorClass(SortFloatComparator.class);
		
		job.setMapperClass(Step3Mapper.class);
		job.setMapOutputKeyClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true);
	}
	
}

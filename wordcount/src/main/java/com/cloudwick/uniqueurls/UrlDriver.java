package com.cloudwick.uniqueurls;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UrlDriver extends Configured implements Tool {
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length != 2)
		{
			System.out.printf("usage:%s[generic options]<input dir> <output dir>\n",getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
		return -1;
		}
		@SuppressWarnings("deprecation")
		Job job=new Job(getConf());
		job.setJarByClass(UrlDriver.class);
		job.setJobName(getClass().getName());
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(UrlMapper.class);
		job.setReducerClass(UrlReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		if(job.waitForCompletion(true))
		{
			return 0;
			
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.exit(ToolRunner.run(new UrlDriver(), args));

	}

}

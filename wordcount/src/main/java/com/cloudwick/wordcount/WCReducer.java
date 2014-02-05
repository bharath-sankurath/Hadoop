package com.cloudwick.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text,IntWritable,Text, IntWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> countlist, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count=0;
		for(IntWritable val:countlist)
		{
			count+=val.get();
		}
		context.write(key,new IntWritable(count));
			
	}
	

}

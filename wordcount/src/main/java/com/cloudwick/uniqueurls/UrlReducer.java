package com.cloudwick.uniqueurls;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlReducer extends Reducer<Text, Text , Text, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text url, Iterable<Text> iplist,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	HashSet <String> hs= new HashSet<String>();
	for(Text val:iplist)
	{
		hs.add(val.toString());
	}
    Iterator<String> iterator = hs.iterator();
	String ip=null;
	while(iterator.hasNext())
	{
		ip+=iterator.next()+",";
	}
    ip+=hs.size(); 
	context.write(url,new Text(ip));
	
	}
}

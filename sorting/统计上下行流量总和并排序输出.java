package wordcountTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sun.org.apache.bcel.internal.generic.NEW;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;

public class WordCount {

	public static class Map extends
			Mapper<Object, Text, LongWritable, LongWritable> {

		private final static IntWritable one = new IntWritable(1); 
		private LongWritable phoneNumber = new LongWritable(); 
		private LongWritable totalFlow = new LongWritable();  //记录总流量

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] array = line.split(" ");
			totalFlow = new LongWritable(Long.parseLong(array[1])
					+ Long.parseLong(array[2]));  //上下行流量相加
			if (array.length == 3) {
				context.write(new LongWritable(Long.parseLong(array[0])), totalFlow);
			} else {
				return;
			}
		}
	}

	public static class Reduce extends
			Reducer<LongWritable, LongWritable, LongWritable, Object> {

		private IntWritable result = new IntWritable();

		public void reduce(LongWritable key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			
			List <Long> totalUserFlow = new ArrayList<Long>();
			for (LongWritable val : values) {
				totalUserFlow.add(val.get());
			}
			
			Object[] arrObjects =totalUserFlow.toArray();
			Arrays.sort(arrObjects);
		
			for(int j=0; j<arrObjects.length; j++){
				context.write(key, arrObjects[j]);
			}
		}
	}
	

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		System.out.println(System.getenv("HADOOP_HOME"));
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		// create a job with name "wordcount"
		Job job = new Job(conf, "userflow");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner
		// job.setCombinerClass(Reduce.class);
		// set output key type

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		 job.setOutputKeyClass(LongWritable.class);
		 job.setOutputValueClass(Object.class);

		// set reduce number
		job.setNumReduceTasks(1);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

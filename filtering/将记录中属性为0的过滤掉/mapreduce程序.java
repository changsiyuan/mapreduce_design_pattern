package wordcountTest;

import java.io.IOException;
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

public class WordCount {

	public static class Map extends Mapper<Object, Text, String, Text> {

		private final static IntWritable one = new IntWritable(1); // type of
																	// output
																	// value
		private KVcount user = new KVcount(); // type of output key
		private Text webPage = new Text();

		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
//      StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token
      
//      while (itr.hasMoreTokens()) {
//        word.set(itr.nextToken());    // set word as each input keyword
//        context.write(word, one);     // create a pair <keyword, 1> 
//      }
    	String line = value.toString();
    	String [] array = line.split(" ");
    	if(array.length==3){
    		user.setphoneNum(Long.parseLong(array[0]));
    		user.setattribute(Integer.parseInt(array[1]));
    		webPage = new Text(array[2]);
    		if(user.attribute!=0){
    			context.write(String.valueOf(user.phoneNum)+"  "+String.valueOf(user.attribute),webPage);
    		}
    		
    	}else{
    		return;
    	}
    }
	}

	//本程序不需要reduce过程
	public static class Reduce extends
			Reducer<KVcount, Text, Text, LongWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			// int sum = 0; // initialize the sum for each keyword
			for (LongWritable val : values) {
				context.write(key, val);
				// sum += val.get();
			}
			// result.set(sum);

			// context.write(key, values); // create a pair <keyword, number of
			// occurences>
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

		job.setMapOutputKeyClass(KVcount.class);
		job.setMapOutputValueClass(Text.class);
		
//		job.setOutputKeyClass(Text.class);
//		// set output value type
//		job.setOutputValueClass(LongWritable.class);

		// set reduce number
		job.setNumReduceTasks(0);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

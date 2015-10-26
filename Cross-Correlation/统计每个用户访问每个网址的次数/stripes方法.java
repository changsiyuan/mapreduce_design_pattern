package wordcount_test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

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

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, LongWritable, Text>{

//    private final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
    private LongWritable phoneNum = new LongWritable();
    private Text webPage = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String line = value.toString();
		String[] array = line.split(" ");
		if(array.length==2){
			phoneNum = new LongWritable(Long.parseLong(array[0]));
			webPage = new Text(array[1]);
			context.write(phoneNum, webPage);
		}else{
			return;
		}
    }
  }

  public static class IntSumReducer
       extends Reducer<LongWritable,Text, String, IntWritable> {

	private IntWritable result = new IntWritable();
	
    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int sum = 0;
      Text webText;
      String webString;
      List <String> webList = new ArrayList<String>();
      Set <String> webSet = new HashSet<String>(); 
      
      //将values迭代器内容放入list中，方便之后重复访问
      for(Text val : values){
    	  webList.add(val.toString());
      }
      //唯一的网址存入webSet
      for (String val : webList) {
    	  webSet.add(val);
      }
      //分别统计当前手机号（key）的每个访问网址的次数并输出
      for(String s: webSet){
    	  for (String web : webList) {
    		  if(s.equals(web)){
    			  sum++;
    		  }
    	  }
    	  result.set(sum);
    	  context.write(key.toString()+" "+s, result);
    	  sum =0;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(String.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
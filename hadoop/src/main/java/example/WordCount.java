package example;  
  
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
  
public class WordCount {  
  
    public static class WordCountMap extends  
            Mapper<LongWritable, Text, Text, IntWritable> {  
  
        private final IntWritable one = new IntWritable(1);  
        private Text word = new Text();  
  
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {  
//        	try {
//				Class.forName("org.apache.hadoop.streaming.AutoInputFormat");
//			} catch (ClassNotFoundException e) {
//				
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				
//			}
            String line = value.toString();  
            StringTokenizer token = new StringTokenizer(line);  
            while (token.hasMoreTokens()) {  
                word.set(token.nextToken());  
                context.write(word, one);  
            }  
        }  
    }  
  
    public static class WordCountReduce extends  
            Reducer<Text, IntWritable, Text, IntWritable> {  
  
        public void reduce(Text key, Iterable<IntWritable> values,  
                Context context) throws IOException, InterruptedException {  
//        	try {
//				Class.forName("org.apache.hadoop.streaming.AutoInputFormat");
//			} catch (ClassNotFoundException e) {
//				
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				
//			}
            int sum = 0;  
            for (IntWritable val : values) {  
                sum += val.get();  
            }  
            context.write(key, new IntWritable(sum));  
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        Job job = new Job(conf);  
        job.setJarByClass(WordCount.class);  
        job.setJobName("wordcount");  
  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
  
        job.setMapperClass(WordCountMap.class);  
        job.setReducerClass(WordCountReduce.class);  
  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        
        String tmpFileName = "/tmp/" + UUID.randomUUID().toString();
        if(args.length == 3 && args[2].trim().equals("local")){
        	FileSystem fs = FileSystem.get(conf);
        	if(!fs.exists(new Path("/tmp/"))){
        		fs.mkdirs(new Path("/tmp/"));
        	}
        	fs.copyFromLocalFile(new Path(args[0]), new Path(tmpFileName));
        	FileInputFormat.addInputPath(job, new Path(tmpFileName));
        }else{
        	FileInputFormat.addInputPath(job, new Path(args[0]));  
        }
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
  
        job.waitForCompletion(true);  
        if(args.length == 3 && args[2].trim().equals("local")){
        	FileSystem fs = FileSystem.get(conf);
        	fs.deleteOnExit(new Path(tmpFileName));
        }
    }  
}


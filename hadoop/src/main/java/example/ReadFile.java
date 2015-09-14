/**
 * Project Name:hadoop
 * File Name:ReadFile.java
 * Package Name:example
 * Date:2015年8月31日下午6:00:38
 *
 */

package example;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * ClassName:ReadFile <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * 调用：hadoop jar example-0.0.1-SNAPSHOT.jar example.ReadFile /r-data/text9 /liuc/output100/
 * Date: 2015年8月31日 下午6:00:38 <br/>
 * 
 * @author blake
 * @version
 * @since SHUNHE 1.0
 * @see
 */
public class ReadFile {

	public static String forecastLength = "3";//默认预测下三个周期的数据
	
	public static Logger log = Logger.getLogger(ReadFile.class);
	
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
//			context.write(key, new Text(UUID.randomUUID().toString()));
//			context.write(key, value);
			String[] valueArr = value.toString().split(";");
			if(valueArr.length == 2){//不符合规范， 直接扔掉。
				Text key_ = new Text(valueArr[0]);
//				//调用R进行计算
//				Runtime rt = Runtime.getRuntime();
//				//生成临时文件路径保存R计算的临时结果
//				String path = "/tmp/poc/" + UUID.randomUUID();
//				File f = new File("/tmp/poc/");
//				if(!f.exists()){
//					rt.exec("mkdir " + "/tmp/poc/");
//				}
//				log.info("---------------------------------------------------------------");
////				log.info("Rscript -e 'fore <- doPOCForecast(c(" + valueArr[1] + ")," + forecastLength + ");write.csv(fore, file=\"" + path + "\");'");
////				Process p = rt.exec("Rscript -e 'fore <- doPOCForecast(c(" + valueArr[1] + ")," + forecastLength + ");write.csv(fore, file=\"" + path + "\");'");
//				Process p = rt.exec("sh /opt/liuc/poc.run.sh " + valueArr[1] + " " + forecastLength + " " + path);
//				log.info(p.waitFor());
//				log.info("---------------------------------------------------------------");
//				List<String> strs = IOUtils.readLines(new FileInputStream(new File(path)));
//				context.write(key_, new Text(Arrays.toString(strs.toArray())));
				
				context.write(key_, new Text(valueArr[1]));
			}
		}
	}

	public static class MyReduce extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context arg2)
				throws IOException, InterruptedException {
//			StringBuilder sb = new StringBuilder("");
//			for (Text val : values) {
//				sb.append(val);
//			}
//			arg2.write(key, new Text(sb.toString()));
			
			for (Text val : values) {
				//调用R进行计算
				Runtime rt = Runtime.getRuntime();
				//生成临时文件路径保存R计算的临时结果
				String path = "/tmp/poc/" + UUID.randomUUID();
				File f = new File("/tmp/poc/");
				if(!f.exists()){
					rt.exec("mkdir " + "/tmp/poc/");
				}
				log.info("---------------------------------------------------------------");
//				log.info("Rscript -e 'fore <- doPOCForecast(c(" + valueArr[1] + ")," + forecastLength + ");write.csv(fore, file=\"" + path + "\");'");
//				Process p = rt.exec("Rscript -e 'fore <- doPOCForecast(c(" + valueArr[1] + ")," + forecastLength + ");write.csv(fore, file=\"" + path + "\");'");
				Process p = rt.exec("sh /opt/liuc/poc.run.sh " + val.toString() + " " + forecastLength + " " + path);
				log.info(p.waitFor());
				log.info("---------------------------------------------------------------");
				List<String> strs = IOUtils.readLines(new FileInputStream(new File(path)));
				arg2.write(key, new Text(Arrays.toString(strs.toArray())));
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 2){
			System.out.println("输入参数个数不正确。");
		}
		Configuration conf = new Configuration();
		
		Job job = new Job(conf);
		job.setJarByClass(ReadFile.class);
		job.setJobName("POC");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int reduceCount = 1000;
		if(args.length >= 3){
			try {
				reduceCount = Integer.valueOf(args[2]);
			} catch (NumberFormatException e) {
				
				e.printStackTrace();
				
			}
		}
		
		job.setNumReduceTasks(reduceCount);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}

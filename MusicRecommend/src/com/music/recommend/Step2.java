package com.music.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 按用户分组，计算所有歌曲的id出现的组合列表，得到用户对歌曲的id的喜爱度得分矩阵<br>
	u13	i160:1,<br>
	u14	i25:1,i223:1,<br>
	u16	i252:1,<br>
	u21	i266:1,<br>
	u24	i64:1,i218:1,i185:1,<br>
	u26	i276:1,i201:1,i348:1,i321:1,i136:1,<br>
 * @author MichaelYip
 */
	public class Step2 {

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);

			job.setJobName("step2");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step2_Mapper.class);
			job.setReducerClass(Step2_Reducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
			Path outpath = new Path(paths.get("Step2Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

		// 如果使用：用戶+歌曲的id，同时作为输出key，更好
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//i1,u2723,click,2014/9/14 9:31
			String[] tokens = value.toString().split(",");
			String item = tokens[0];
			String user = tokens[1];
			String action = tokens[2];
			Text k = new Text(user);
			Integer rv = StartRun.R.get(action);
			// if(rv!=null){
			Text v = new Text(item + ":" + rv.intValue());
			//u2750	i160:1
			context.write(k, v);
		}
	}

	static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> i, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> r = new HashMap<String, Integer>();

			//迭代同一用户关注的歌曲的id
			for (Text value : i) {
				//u2750	i160:1
				String[] vs = value.toString().split(":");
				String item = vs[0];
				Integer action = Integer.parseInt(vs[1]);
				action = ((Integer) (r.get(item) == null ? 0 : r.get(item))).intValue() + action;
				r.put(item, action);
			}
			StringBuffer sb = new StringBuffer();
			for (Entry<String, Integer> entry : r.entrySet()) {
				sb.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");
			}

			//u2756	i105:1,i79:1,i341:1,i319:1,i332:1,i160:1,i342:1,i94:1,
			context.write(key, new Text(sb.toString()));
		}
	}
}

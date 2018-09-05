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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 把同现矩阵和得分矩阵相乘<br>
 * 利用MR原语特征，按歌曲分组<br>
 * 这样相同歌曲的同现列表和所有用户对该歌曲的评分进到一个reduce中<br>
 * 
 * 
 * @author MichaelYip
 */
public class Step4 {

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step4");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step4_Mapper.class);
			job.setReducerClass(Step4_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job,
					new Path[] { new Path(paths.get("Step4Input1")),
							new Path(paths.get("Step4Input2")) });
			Path outpath = new Path(paths.get("Step4Output"));
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

	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;// A同现矩阵 or B得分矩阵

		// 每个maptask，初始化时调用一次
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// 判断读的数据集

			System.out.println(flag + "**********************");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());

			if (flag.equals("step3")) {// 同现矩阵
				// 样本:  i100:i181	1
				//       i100:i184	2
				String[] v1 = tokens[0].split(":");
				String itemID1 = v1[0];
				String itemID2 = v1[1];
				String num = tokens[1];

				Text k = new Text(itemID1);// 以前一个歌曲id为key 比如i100
				Text v = new Text("A:" + itemID2 + "," + num);// A:i109,1
				// 样本:  i100	A:i181,1
				context.write(k, v);

			} else if (flag.equals("step2")) {// 用户对歌曲id喜爱得分矩阵
				// 样本:  u24  i64:1,i218:1,i185:1,
				String userID = tokens[0];
				for (int i = 1; i < tokens.length; i++) {
					String[] vector = tokens[i].split(":");
					String itemID = vector[0];// 歌曲idid
					String pref = vector[1];// 喜爱分数

					Text k = new Text(itemID); // 以歌曲id为key 比如：i100
					Text v = new Text("B:" + userID + "," + pref); // B:u401,2
					// 样本:  i64	B:u24,1
					context.write(k, v);
				}
			}
		}
	}

	static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// A同现矩阵 or B得分矩阵
			// 某一个歌曲id，针对它和其他所有歌曲id的同现次数，都在mapA集合中
			Map<String, Integer> mapA = new HashMap<String, Integer>();
			//和该歌曲id（key中的itemID）同现的其他歌曲id的同现集合//
			//其他歌曲idID为map的key，同现数字为值
			
			Map<String, Integer> mapB = new HashMap<String, Integer>();
			//该歌曲id（key中的itemID），所有用户的推荐权重分数

			for (Text line : values) {
				String val = line.toString();
				if (val.startsWith("A:")) {// 表示歌曲id同现数字// 样本:  i100	A:i181,1
					String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
					try {
						mapA.put(kv[0], Integer.parseInt(kv[1]));//mapA:"i181" -> "1"
					} catch (Exception e) {
						e.printStackTrace();
					}

				} else if (val.startsWith("B:")) {// 样本:  i64	B:u24,1
					String[] kv = Pattern.compile("[\t,]").split(
							val.substring(2));
					try {
						mapB.put(kv[0], Integer.parseInt(kv[1]));//mapB:"u24" -> "1"
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			
			double result = 0;
			//同现矩阵A
			Iterator<String> iter = mapA.keySet().iterator();
			//MR原语特征，这里只有一种歌曲的同现列表
			while (iter.hasNext()) {
				String mapk = iter.next();// itemID

				int num = mapA.get(mapk).intValue();
				Iterator<String> iterb = mapB.keySet().iterator();
				//MR原语特征，这里是所有用户的同一歌曲的评分，迭代之
				while (iterb.hasNext()) {//迭代用户名
					String mapkb = iterb.next();// userID
					int pref = mapB.get(mapkb).intValue();
					//注意这里的计算思维理解：
						//针对A歌曲
						//使用用户对A歌曲的分值
						//逐一乘以与A歌曲有同现的歌曲的次数
						//但是计算推荐向量的时候需要的是A歌曲同现的歌曲，用同现次数乘以各自的分值
					result = num * pref;// 矩阵乘法相乘计算

//					Text k = new Text(mapkb);
//					Text v = new Text(mapk + "," + result);
//				//  结果样本:  u2723    	i9,8.0
//					context.write(k, v);
					
					Text k = new Text(mapkb+","+mapk);
					Text v = new Text( key.toString() + "," + result);
					//key:101
					//  结果样本:   u3,101   101,4.0   *
					//  结果样本:   u3,102   101,4.0   
					//  结果样本:   u3,103   101,4.0
					//key:102
					//  结果样本:   u3,101   102,4.0   *
					//  结果样本:   u3,102   102,4.0   
					//  结果样本:   u3,103   102,4.0
					
					context.write(k, v);
				}
			}
		}
	}
}
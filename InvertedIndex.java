import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class TokenizerMapper extends Mapper<Object, Text,Text, Text>{	
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text docid=new Text();
			docid.set(itr.nextToken());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, docid);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		private Text doc_count=new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<Text, Text> map=new HashMap<Text, Text>();
			
			for (Text val : values) {
				
				if(val.toString().contains(":")) {
					String[] p=val.toString().split(":");
					Text k=new Text(p[0]);
					Text v=new Text(p[1]);
					map.put(k, v);
				}
				else {
					
					if(map.containsKey(val)) {
						Text t=new Text(Integer.toString((Integer.parseInt(map.get(val).toString())+1)));
						map.put(val,t);
					}
					else {
						Text t=new Text(Integer.toString(1));
						map.put(val,t);
					}
				}
			}	
			
			StringBuilder sb =new StringBuilder();
			for(Map.Entry<Text,  Text> m : map.entrySet()) {
				sb.append(m.getKey() +":"+m.getValue()+" ");
			}
			
			doc_count.set(sb.toString());
			context.write(key, doc_count);
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(InvertedIndex.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
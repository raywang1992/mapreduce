package usertag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TagDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("country1",args[2]);
		conf.set("country2",args[3]);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		Job job = new Job(conf, "tag owner inverted list with combiner");
		job.setNumReduceTasks(1);
		job.setJarByClass(TagDriver.class);
		job.setMapperClass(TagMapper.class);
		job.setReducerClass(TagReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

package usertag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*Driver of the program

author Rui Wang
*/
public class VideoDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("country1",args[2]);					//input the first Country
		conf.set("country2",args[3]);					//input the second Country
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		Job job = new Job(conf, "video category");
		job.setNumReduceTasks(1);
		job.setJarByClass(VideoDriver.class);
		job.setMapperClass(VideoMapper.class);
		job.setReducerClass(VideoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

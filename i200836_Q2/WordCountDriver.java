package pack;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountDriver {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] PATHS = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job ArtJob= Job.getInstance(conf, "Assignment 2");
		ArtJob.setJarByClass(WordCount.class);
		ArtJob.setMapperClass(WordCount.class);
		ArtJob.setCombinerClass(WordcountReducer.class);
		ArtJob.setReducerClass(WordcountReducer.class);
		ArtJob.setOutputKeyClass(Text.class);
		ArtJob.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < PATHS .length - 1; ++i){
			FileInputFormat.addInputPath(ArtJob, new Path(PATHS[i]));
			}
		FileOutputFormat.setOutputPath(ArtJob, new Path(PATHS[PATHS.length -1]));
		System.exit(ArtJob.waitForCompletion(true) ? 0 : 1);
		}
}

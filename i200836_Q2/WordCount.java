package pack; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class WordCount extends Mapper<Object, Text, Text, IntWritable>{

	
	private final static IntWritable ONE = new IntWritable(1);


    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      
        String line = value.toString();
        String[] fields = line.split(",");
        
        String[] authors = fields[1].replaceAll("\\[|\\]|'", "").split(", ");
        
        for (int i = 0; i < authors.length; i++) {
            for (int j = i + 1; j < authors.length; j++) {
                String author1 = authors[i].trim();
                String author2 = authors[j].trim();
                
                if (author1.length() > 0 && author2.length() > 0) {
                    context.write(new Text(author1 + "," + author2), ONE);
                  
                }
            }
        }
    }
}

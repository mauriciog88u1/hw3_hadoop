package csx55.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {
        System.out.println("ARGS LENGTH "+ args.length);      
        if (args.length > 2) {
            int questionNumber = Integer.parseInt(args[2]);  
            conf.setInt("question.number", questionNumber);
        }
        Job job = Job.getInstance(conf, "Homework 3 Million Song Dataset");
        job.setJarByClass(Driver.class);
        job.setMapperClass(SongMapper.class);
        job.setReducerClass(SongReducer.class);
        job.setCombinerClass(SongReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
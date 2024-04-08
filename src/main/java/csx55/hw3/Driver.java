package csx55.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Usage: Driver <metadata path> <analysis path> <output path> <question number>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        int questionNumber = Integer.parseInt(args[4]);
        conf.setInt("question.number", questionNumber);
        // Metadata.txt
        Job job = Job.getInstance(conf, "Homework 3 Million Song Dataset metadata analysis");
        job.setJarByClass(Driver.class);
        job.setMapperClass(SongMapper.class);
        job.setReducerClass(SongReducer.class);
        job.setCombinerClass(SongReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Analysis.txt
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        Job job2 = Job.getInstance(conf, "Homework 3 Million Song Dataset anlysis");
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(AnalysisMapper.class);
        job2.setReducerClass(AnalysisReducer.class);
        job2.setCombinerClass(AnalysisReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

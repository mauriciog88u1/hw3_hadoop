package csx55.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Usage: Driver <metadata path> <analysis path> <output path> <question number>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        int questionNumber = Integer.parseInt(args[4]);
        conf.setInt("question.number", questionNumber);

        switch (questionNumber) {
            case 1:
                questionOne(args, conf);
                break;
            case 2:
                questionTwo(args, conf);
                break;
            default:
                break;
        }
    }

    private static void questionTwo(String[] args, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job q2_1 = Job.getInstance(conf, "Homework 3 Million Song Dataset metadata analysis");
        q2_1.setJarByClass(Driver.class);
        q2_1.setMapperClass(MetadataMapper.class);
        q2_1.setReducerClass(SongReducer.class);
        q2_1.setOutputKeyClass(Text.class);
        q2_1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(q2_1, new Path(args[1]));
        FileOutputFormat.setOutputPath(q2_1, new Path(args[3]));

        Job q2_2 = Job.getInstance(conf, "Homework 3 Million Song Dataset analysis");
        q2_2.setJarByClass(Driver.class);
        q2_2.setMapperClass(AnalysisMapper.class);
        q2_2.setReducerClass(SongReducer.class);
        q2_2.setOutputKeyClass(Text.class);
        q2_2.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(q2_2, new Path(args[2]));
        FileOutputFormat.setOutputPath(q2_2, new Path(args[3] + "2"));

        System.exit(q2_1.waitForCompletion(true) && q2_2.waitForCompletion(true) ? 0 : 1);
    }

    private static void questionOne(String[] args, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job q1_1 = Job.getInstance(conf, "Homework 3 Million Song Dataset metadata analysis");
        q1_1.setJarByClass(Driver.class);
        q1_1.setMapperClass(SongMapper.class);
        q1_1.setReducerClass(SongReducer.class);
        q1_1.setCombinerClass(SongReducer.class);
        q1_1.setOutputKeyClass(Text.class);
        q1_1.setOutputValueClass(IntWritable.class);

        System.exit(q1_1.waitForCompletion(true) ? 0 : 1);
    }
}
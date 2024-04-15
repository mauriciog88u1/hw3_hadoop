package csx55.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Driver {

    private static final String METADATA_INPUT_PATH = "/hw3/metadata.txt";
    private static final String ANALYSIS_INPUT_PATH = "/hw3/analysis.txt";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Driver <question number> <output path>");
            System.exit(-1);
        }
        System.out.printf("Question %s output will be saved to %s%n", args[0], args[1]);

        int questionNumber = Integer.parseInt(args[0]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Song Data Analysis Q" + questionNumber);
        job.setJarByClass(Driver.class);

        switch (questionNumber) {
            case 1:
                job.setNumReduceTasks(1);
                job.setMapperClass(q1Mapper.class);
                job.setReducerClass(q1Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(job, new Path(METADATA_INPUT_PATH));
                break;
            case 2:
                job.setMapperClass(q2Mapper.class);
                job.setReducerClass(q2Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                MultipleInputs.addInputPath(job, new Path(METADATA_INPUT_PATH), TextInputFormat.class, q2Mapper.class);
                MultipleInputs.addInputPath(job, new Path(ANALYSIS_INPUT_PATH), TextInputFormat.class, q2Mapper.class);
                break;
            case 3:
                System.out.println("Question 3: What is the song with the highest hotttnesss (popularity) score?");
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);  // Update this to DoubleWritable
            
                MultipleInputs.addInputPath(job, new Path(ANALYSIS_INPUT_PATH), TextInputFormat.class, q3AnalysisMapper.class);
                MultipleInputs.addInputPath(job, new Path(METADATA_INPUT_PATH), TextInputFormat.class, q3MetaMapper.class);
            
                job.setReducerClass(q3Reducer.class);
            
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                break;
            case 4:
                System.out.println("Question 4:Which artist has the highest total time spent fading in their songs?");
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, new Path(ANALYSIS_INPUT_PATH), TextInputFormat.class, q4AnalysisMapper.class);
                MultipleInputs.addInputPath(job, new Path(METADATA_INPUT_PATH), TextInputFormat.class, q4MetaMapper.class);

                job.setReducerClass(q4Reducer.class);

                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                break;

            default:
                throw new IllegalStateException("Unexpected value: " + questionNumber);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

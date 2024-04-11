package csx55.hw3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Driver {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SongDataDriver <question number> <analysis input path> <metadata input path> <output path>");
            System.exit(-1);
        }

        int questionNumber = Integer.parseInt(args[0]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Song Data Analysis Q" + questionNumber);
        job.setJarByClass(Driver.class);

        switch (questionNumber) {
            case 1:
                job.setMapperClass(q1Mapper.class);
                job.setReducerClass(q1Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[1]));
                break;
            case 2:
                job.setMapperClass(q1Mapper.class);
                job.setReducerClass(q2Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, q2Mapper.class);
                MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, q2Mapper.class);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + questionNumber);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

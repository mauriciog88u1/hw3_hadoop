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
            case 5:
                System.out.println("Q5. What is the longest song(s)? The shortest song(s)? The song(s) of median length?");
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, new Path(ANALYSIS_INPUT_PATH), TextInputFormat.class,q5AnalysisMapper.class);
                MultipleInputs.addInputPath(job, new Path(METADATA_INPUT_PATH), TextInputFormat.class,q5MetaMapper.class);

                job.setReducerClass(q5Reducer.class);
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                break;
            case 6:
                System.out.println("What are the 10 most energetic and danceable songs? List them in descending order");
                job.setOutputKeyClass(Text.class);
                job.setOutputKeyClass(Text.class);

                MultipleInputs.addInputPath(job, new Path(ANALYSIS_INPUT_PATH), TextInputFormat.class, q6AnalysisMapper.class);
                MultipleInputs.addInputPath(job, new Path(METADATA_INPUT_PATH), TextInputFormat.class, q5MetaMapper.class);
                job.setReducerClass(q6Reducer.class);


                break;
            case 7:
            System.out.println("Create segment data for the average song. Include start time, pitch, timbre, max loudness, max loudness time, and start loudness");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, new Path(ANALYSIS_INPUT_PATH), TextInputFormat.class, q7AnalysisMapper.class);
            MultipleInputs.addInputPath(job, new Path(METADATA_INPUT_PATH), TextInputFormat.class, q5MetaMapper.class);
            job.setReducerClass(q7Reducer.class);
            break;
            case 8:
                System.out.println("Q8. Which artist is the most generic? Which artist is the most unique?");
                // I define generic as a artist who has most songs in dataset and who has mosrt artist terms so a comb
                // of both
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(job, new Path(METADATA_INPUT_PATH));
                job.setMapperClass(q8Mapper.class);
                job.setReducerClass(q8Reducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);
                



                break;
            case 9:
            // String promprString = "
            //     Imagine a song with a higher hotnesss score than the song in your answer to Q3. List this
            //     songs tempo, time signature, danceability, duration, mode, energy, key, loudness, when it
            //     stops fading in, when it starts fading out, and which terms describe the artist who made it.
            //     Give both the song and the artist who made it unique names.
            //         ";


                break;
            case 10:
                System.out.println("Q10. Create your own question. my question is to find the song that could be played at the retro tiktok rizz party");
                break;

        
            default:
                throw new IllegalStateException("Unexpected value: " + questionNumber);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

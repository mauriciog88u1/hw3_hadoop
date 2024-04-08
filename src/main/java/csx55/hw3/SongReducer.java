package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class SongReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text maxArtist = new Text();
    private int maxCount = 0;
    private MultipleOutputs<Text, IntWritable> mos;

    private int questionNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.questionNumber = context.getConfiguration().getInt("question.number", 1);
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        switch (questionNumber) {
            case 1:
                processQuestion1(key, values, context);
                break;
            case 2:
                processQuestion2(key, values, context);
                break;
            default:
                break;
        }
    }

    private void processQuestion1(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        if (sum > maxCount) {
            maxCount = sum;
            maxArtist.set(key);
        }
        String description = String.format("Artist with maximum count is %s with count %d", maxArtist.toString(), maxCount);
        context.write(new Text(description), null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {


    }
    private void processQuestion2(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    }
}
package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q8Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text maxArtist = new Text();
    private int maxValue = Integer.MIN_VALUE;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }

        if (sum > maxValue) {
            maxValue = sum;
            maxArtist.set(key);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String statment = "Most generic artist is: " + maxArtist.toString();
        context.write(new Text(statment), new IntWritable(maxValue));
    }
}

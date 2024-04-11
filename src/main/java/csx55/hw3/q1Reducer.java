package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text maxArtistQ1 = new Text();
    private int maxCountQ1 = 0;

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        if (sum > maxCountQ1) {
            maxCountQ1 = sum;
            maxArtistQ1.set("Artist: " + key.toString() + " Count: " + sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxArtistQ1, new IntWritable(maxCountQ1));
    }
}

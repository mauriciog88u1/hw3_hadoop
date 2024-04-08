package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text maxArtist = new Text();
    private IntWritable maxCount = new IntWritable(0);
    private int questionNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.questionNumber = context.getConfiguration().getInt("question.number", 1);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        if (sum > maxCount.get()) {
            maxArtist.set(key);
            maxCount.set(sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text result = new Text("Question " + questionNumber + ": " + maxArtist.toString());
        context.write(result, maxCount);
    }
}

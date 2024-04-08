package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text result = new Text();
    private IntWritable count = new IntWritable();

    private int questionNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.questionNumber = context.getConfiguration().getInt("question.number", 1);
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
        count.set(sum);
        context.write(key, count);
    }
    

    private void processQuestion2(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    }

}

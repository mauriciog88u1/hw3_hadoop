package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongReducer extends Reducer<Text, Text, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private Text maxArtist = new Text();
    private int maxCount = 0;
    private int questionNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.questionNumber = context.getConfiguration().getInt("question.number", 1);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

    private void processQuestion1(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text val : values) {
            try {
                sum += Integer.parseInt(val.toString());
            } catch (NumberFormatException e) {
                // Ignore non-integer values
            }
        }
        if (sum > maxCount) {
            maxCount = sum;
            maxArtist.set(key);
        }
    }

    private void processQuestion2(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistName = "";
        int maxLoudness = Integer.MIN_VALUE;

        for (Text val : values) {
            String value = val.toString();
            try {
                int loudness = Integer.parseInt(value);
                if (loudness > maxLoudness) {
                    maxLoudness = loudness;
                    artistName = key.toString();
                }
            } catch (NumberFormatException e) {
                artistName = value;
            }
        }

        if (!artistName.isEmpty()) {
            context.write(new Text(artistName), new IntWritable(maxLoudness));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (questionNumber == 1) {
            context.write(maxArtist, new IntWritable(maxCount));
        }
    }
}
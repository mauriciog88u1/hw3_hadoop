package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SongMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text artistId = new Text();
    private int questionNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.questionNumber = context.getConfiguration().getInt("question.number", 1);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\\|");

        if (data.length > 0) {
            switch (questionNumber) {
                case 1:
                    processQuestion1(data, context);
                    break;
                case 2:
                    processQuestion2(data, context);
                    break;
                default:
                    break;
            }
        }
    }

    private void processQuestion1(String[] data, Context context) throws IOException, InterruptedException {
        Text description = new Text();

        if (data.length > ARTIST_ID_INDEX) {
            description.set("Most popular artist(Question 1:");
            artistId.set(data[ARTIST_ID_INDEX]);
            context.write(artistId, one);
        }
        
    }

    private void processQuestion2(String[] data, Context context) throws IOException, InterruptedException {
   
    }
    // Add methods for other questions as needed
}

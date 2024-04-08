package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.SongData.ARTIST_ID_INDEX;

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

        switch (questionNumber) {
            case 1:
                if (data.length > ARTIST_ID_INDEX) {
                    artistId.set(data[ARTIST_ID_INDEX]);
                    context.write(artistId, one);
                }
                break;
            case 2:
                System.out.println("Question 2");
                break;
        }
    }
}

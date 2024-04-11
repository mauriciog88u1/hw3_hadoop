package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.utils.metadata.ARTIST_NAME_INDEX;

public class q1Mapper extends Mapper<Object, Text, Text, IntWritable>{
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
        artistId.set(data[ARTIST_NAME_INDEX]);
        context.write(artistId, one);
        System.out.println("Finished question " + questionNumber + " mapper");
    }


}

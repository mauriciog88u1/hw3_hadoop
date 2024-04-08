package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.SongData.ARTIST_ID_INDEX;

public class SongMapper extends Mapper<Object, Text, Text, IntWritable> {
                                //    KEYIN, VALUEIN, KEYOUT, VALUEOUT

    private final static IntWritable one = new IntWritable(1);
    private final Text artistId = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\\|");
        Text description = new Text();

        if (data.length > ARTIST_ID_INDEX) {
            description.set("Most popular artist(Question 1:");
            artistId.set(data[ARTIST_ID_INDEX]);
            context.write(artistId, one);
        }
    }
}

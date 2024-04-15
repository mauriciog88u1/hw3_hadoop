package csx55.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class q3MetaMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int SONG_ID_INDEX_METADATA = 7;  // Based on metadata structure
    private static final int TITLE_INDEX = 8;             // Based on metadata structure

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\|");

        if (parts.length > TITLE_INDEX) {
            String songId = parts[SONG_ID_INDEX_METADATA].trim();
            String title = parts[TITLE_INDEX].trim();
            context.write(new Text(songId), new Text("title:" + title));
        }
    }
}

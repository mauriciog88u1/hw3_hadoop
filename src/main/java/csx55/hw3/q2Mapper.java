package csx55.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class q2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int SONG_ID_INDEX_METADATA = 7;  // Based on metadata structure
    private static final int ARTIST_NAME_INDEX = 6;       // Based on metadata structure
    private static final int SONG_ID_INDEX_ANALYSIS = 0;  // Based on analysis structure
    private static final int LOUDNESS_INDEX = 9;          // Based on analysis structure

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\|");

        if (parts.length > LOUDNESS_INDEX && isNumeric(parts[LOUDNESS_INDEX])) {
            String songId = parts[SONG_ID_INDEX_ANALYSIS];
            String loudness = parts[LOUDNESS_INDEX];
            context.write(new Text(songId), new Text("Loudness:" + loudness));
        } else if (parts.length > ARTIST_NAME_INDEX) {
            String songId = parts[SONG_ID_INDEX_METADATA];
            String artistName = parts[ARTIST_NAME_INDEX];
            context.write(new Text(songId), new Text("Artist:" + artistName));
        }
    }

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

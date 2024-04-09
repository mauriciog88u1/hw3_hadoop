package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.SongData.*;

public class MetadataMapper extends Mapper<Object, Text, Text, Text> {
    private Text songID = new Text();
    private Text artistName = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\\|");
        songID.set(data[SONG_ID_INDEX_METADATA]);
        artistName.set(data[ARTIST_NAME_INDEX]);
        context.write(songID, artistName);
    }
}
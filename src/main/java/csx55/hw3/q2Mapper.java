package csx55.hw3;

import csx55.hw3.utils.Analysis;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.utils.metadata.ARTIST_NAME_INDEX;
import static csx55.hw3.utils.metadata.SONG_ID_INDEX;

public class q2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\|"); // Assuming fields are separated by '|'

        // Check if the line is from analysis.txt or metadata.txt based on number of fields or content structure
        if (parts.length > Analysis.LOUDNESS_INDEX) { // Check if it's analysis data
            String songId = parts[Analysis.SONG_ID_INDEX_ANALYSIS];
            String loudness = parts[Analysis.LOUDNESS_INDEX];
            context.write(new Text(songId), new Text("Loudness:" + loudness));
        } else if (parts.length > ARTIST_NAME_INDEX) { // Check if it's metadata
            String songId = parts[SONG_ID_INDEX];
            String artistName = parts[ARTIST_NAME_INDEX];
            context.write(new Text(songId), new Text("Artist:" + artistName));
        }
    }
}

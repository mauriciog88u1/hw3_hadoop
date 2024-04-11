package csx55.hw3;

import csx55.hw3.utils.metadata;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.utils.Analysis.SONG_HOTTTNESSS_INDEX;
import static csx55.hw3.utils.Analysis.SONG_ID_INDEX_ANALYSIS;

public class q3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\\|");
        String songId;
        String outputValue;
        if (data.length == SONG_HOTTTNESSS_INDEX + 1) {
            // This is the analysis data
            songId = data[SONG_ID_INDEX_ANALYSIS];
            outputValue = "A|" + data[SONG_HOTTTNESSS_INDEX];
        } else {
            songId = data[metadata.SONG_ID_INDEX];
            outputValue = "M|" + data[metadata.TITLE_INDEX];
        }
        context.write(new Text(songId), new Text(outputValue));
    }
}
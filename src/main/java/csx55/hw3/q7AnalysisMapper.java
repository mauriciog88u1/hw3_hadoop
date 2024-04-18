package csx55.hw3;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import static csx55.hw3.utils.Analysis.*;

public class q7AnalysisMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) {
        String[] parts = value.toString().split("\\|");

        try {
            if (parts.length > SEGMENTS_LOUDNESS_START_INDEX) {
                String songId = parts[SONG_ID_INDEX_ANALYSIS];
                
                context.write(new Text(songId ), new Text( "StartTime:" + parts[SEGMENTS_START_INDEX]));
                context.write(new Text(songId ), new Text( "Pitches: "+ parts[SEGMENTS_PITCHES_INDEX]));
                context.write(new Text(songId ), new Text( "Timbre:" + parts[SEGMENTS_TIMBRE_INDEX]));
                context.write(new Text(songId ), new Text("MaxLoudness"+ parts[SEGMENTS_LOUDNESS_MAX_INDEX]));
                context.write(new Text(songId ), new Text("MaxLoudnessTime:"+parts[SEGMENTS_LOUDNESS_MAX_TIME_INDEX]));
                context.write(new Text(songId ), new Text("StartLoudness"+parts[SEGMENTS_LOUDNESS_START_INDEX]));
            }
        } catch (Exception e) {
            System.err.println("Error processing: " + value.toString());
            e.printStackTrace();
        }
    }
}

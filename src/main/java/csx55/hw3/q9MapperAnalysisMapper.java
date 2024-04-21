package csx55.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import static csx55.hw3.utils.Analysis.*;

import java.io.IOException;

public class q9MapperAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\|");

        if (parts.length > TATUMS_CONFIDENCE_INDEX && isNumeric(parts[SONG_HOTTTNESSS_INDEX]) && Double.parseDouble(parts[SONG_HOTTTNESSS_INDEX]) >= 1.0) {
            String attributes = String.format("tempo:%s,timeSig:%s,danceability:%s,duration:%s,mode:%s,energy:%s,key:%s,loudness:%s,fadeInEnd:%s,fadeOutStart:%s",
                                              parts[TEMPO_INDEX], parts[TIME_SIGNATURE_INDEX], parts[DANCEABILITY_INDEX], parts[DURATION_INDEX],
                                              parts[MODE_INDEX], parts[ENERGY_INDEX], parts[KEY_INDEX], parts[LOUDNESS_INDEX],
                                              parts[END_OF_FADE_IN_INDEX], parts[START_OF_FADE_OUT_INDEX]);
            context.write(new Text(parts[SONG_ID_INDEX_ANALYSIS]), new Text(attributes));
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

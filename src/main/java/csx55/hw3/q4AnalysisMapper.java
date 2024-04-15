package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import static csx55.hw3.utils.Analysis.*;

public class q4AnalysisMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length > END_OF_FADE_IN_INDEX && parts.length > DURATION_INDEX && parts.length > START_OF_FADE_OUT_INDEX) {
            String songId = parts[SONG_ID_INDEX_ANALYSIS].trim();
            double fade_in = parts[END_OF_FADE_IN_INDEX].trim().equalsIgnoreCase("nan") ? 0.0 : Double.parseDouble(parts[END_OF_FADE_IN_INDEX].trim());
            double totalDuration = parts[DURATION_INDEX].trim().equalsIgnoreCase("nan") ? 0.0 : Double.parseDouble(parts[DURATION_INDEX].trim());
            double startOfFadeOut = parts[START_OF_FADE_OUT_INDEX].trim().equalsIgnoreCase("nan") ? 0.0 : Double.parseDouble(parts[START_OF_FADE_OUT_INDEX].trim());
            
            double fadeOutDuration = totalDuration - startOfFadeOut;
            

            if (fade_in > 0 && fadeOutDuration > 0) {
                double totalFadeDuration = fade_in + fadeOutDuration;
                context.write(new Text(songId), new Text("fade:" + totalFadeDuration));
            }
        }
    }
}

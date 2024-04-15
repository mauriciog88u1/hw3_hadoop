package csx55.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class q3AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final int SONG_ID_INDEX_ANALYSIS = 0;  // Adjust the index according to your file structure
    private static final int HOTNESS_INDEX = 1;           // Adjust the index for hotness according to your file structure

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\|");

        if (parts.length > HOTNESS_INDEX && isNumeric(parts[HOTNESS_INDEX])) {
            String songId = parts[SONG_ID_INDEX_ANALYSIS].trim();
            String hotness = parts[HOTNESS_INDEX].trim().equalsIgnoreCase("nan") ? "0" : parts[HOTNESS_INDEX].trim();
            context.write(new Text(songId), new Text("hotness:" + hotness));
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

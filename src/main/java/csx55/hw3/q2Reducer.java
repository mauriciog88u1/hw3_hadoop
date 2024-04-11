package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class q2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double maxLoudness = Double.MIN_VALUE;
        String artistName = "";
        Map<String, Double> songLoudnessMap = new HashMap<>();

        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts[0].equals("Loudness")) {
                double loudness = Double.parseDouble(parts[1]);
                if (loudness > maxLoudness) {
                    maxLoudness = loudness;
                    songLoudnessMap.put(key.toString(), loudness);
                }
            } else if (parts[0].equals("Artist")) {
                artistName = parts[1];
            }
        }

        if (!artistName.isEmpty() && maxLoudness != Double.MIN_VALUE) {
            context.write(new Text(artistName), new Text("Max Loudness: " + maxLoudness));
        }
    }
}

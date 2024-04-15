package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class q4Reducer extends Reducer<Text, Text, Text, Text> {
    private HashMap<String, Double> artistToTotalFadeTime = new HashMap<>();
    private HashMap<String, String> songToArtistMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String artistName = null;
        double fadeTime = 0.0;
        boolean fadeTimeSet = false;

        for (Text value : values) {
            String val = value.toString();
            String[] parts = val.split(":", 2);  

            if (parts[0].equals("fade") && parts.length > 1) {
                try {
                    fadeTime = Double.parseDouble(parts[1]);
                    fadeTimeSet = true;
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing fade time: " + parts[1]);
                }
            } else if (parts[0].equals("artist") && parts.length > 1) {
                artistName = parts[1];
                songToArtistMap.put(key.toString(), artistName);
            }
        }

        if (artistName != null && fadeTimeSet) {
            artistToTotalFadeTime.merge(artistName, fadeTime, Double::sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String maxArtist = null;
        double maxFadeTime = -1;
        for (HashMap.Entry<String, Double> entry : artistToTotalFadeTime.entrySet()) {
            if (entry.getValue() > maxFadeTime) {
                maxFadeTime = entry.getValue();
                maxArtist = entry.getKey();
            }
        }

        if (maxArtist != null) {
            context.write(new Text(maxArtist), new Text("Total Fade Time: " + maxFadeTime));
        }
    }
}

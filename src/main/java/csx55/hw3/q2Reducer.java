package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class q2Reducer extends Reducer<Text, Text, Text, Text> {
    private final Map<String, String> songArtistMap = new HashMap<>();
    private final Map<String, LoudnessData> artistLoudnessMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(":");

            if (parts[0].equals("Loudness")) {
                double loudness = Double.parseDouble(parts[1]);
                String artistName = songArtistMap.get(key.toString());
                if (artistName != null) {
                    artistLoudnessMap.compute(artistName, (k, v) -> {
                        if (v == null) return new LoudnessData(loudness, 1);
                        v.sumLoudness += loudness;
                        v.count++;
                        return v;
                    });
                }
            } else if (parts[0].equals("Artist")) {
                songArtistMap.put(key.toString(), parts[1]);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double maxAverageLoudness = Double.NEGATIVE_INFINITY;
        String loudestArtist = "";

        for (Map.Entry<String, LoudnessData> entry : artistLoudnessMap.entrySet()) {
            LoudnessData data = entry.getValue();
            double averageLoudness = data.sumLoudness / data.count;

            if (averageLoudness > maxAverageLoudness) {
                maxAverageLoudness = averageLoudness;
                loudestArtist = entry.getKey();
            }
        }



        if (!loudestArtist.isEmpty()) {
            context.write(new Text(loudestArtist), new Text("Average Loudness: " + maxAverageLoudness));
        }
    }

    private static class LoudnessData {
        double sumLoudness;
        int count;

        LoudnessData(double sumLoudness, int count) {
            this.sumLoudness = sumLoudness;
            this.count = count;
        }
    }
}

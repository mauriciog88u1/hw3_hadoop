package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class q3Reducer extends Reducer<Text, Text, Text, Text> {
    private HashMap<String, String> songTitles = new HashMap<>();
    private double maxScore = Double.MIN_VALUE;
    private String hottestSongId = "";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts[0].equals("hotness")) {
                double score = Double.parseDouble(parts[1]);
                if (score > maxScore) {
                    maxScore = score;
                    hottestSongId = key.toString();
                }
            } else if (parts[0].equals("title")) {
                songTitles.put(key.toString(), parts[1]);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (!hottestSongId.isEmpty() && songTitles.containsKey(hottestSongId)) {
            context.write(new Text(songTitles.get(hottestSongId)), new Text(String.format("Highest Hotness: %.2f", maxScore)));
        }
    }
}

package csx55.hw3;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static csx55.hw3.utils.Analysis.*;
import static csx55.hw3.utils.metadata.*;

public class q5Reducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Double> songDuration = new HashMap<>();
    private Map<String, String> songArtist = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts.length > 1) {
                try {
                    if ("duration".equals(parts[0])) {
                        double duration = Double.parseDouble(parts[1]);
                        songDuration.put(key.toString(), duration);
                    }
                    if ("title".equals(parts[0])) {
                        songArtist.put(key.toString(), parts[1]);
                    }
                } catch (NumberFormatException e) {
                    continue;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (songDuration.isEmpty()) {
            return;  // No songs to process
        }

        List<Double> durations = new ArrayList<>(songDuration.values());
        Collections.sort(durations);
        double max = Collections.max(durations);
        double min = Collections.min(durations);
        double median = calculateMedian(durations);

        String maxSong = getKeyByValue(songDuration, max);
        String minSong = getKeyByValue(songDuration, min);
        String medianSong = getKeyByValue(songDuration, median);

        context.write(new Text("Longest song"), new Text( songArtist.get(maxSong) + " with duration " + max));
        context.write(new Text("Shortest song"), new Text( songArtist.get(minSong) + " with duration " + min));
        context.write(new Text("Median Song"),new Text(songArtist.get(medianSong) + " with duration " + median));

  
    }

    private double calculateMedian(List<Double> durations) {
        int size = durations.size();
        if (size % 2 == 0) {
            return (durations.get(size / 2) + durations.get(size / 2 - 1)) / 2.0;
        } else {
            return durations.get(size / 2);
        }
    }

    private String getKeyByValue(Map<String, Double> map, Double value) {
        for (Map.Entry<String, Double> entry : map.entrySet()) {
            if (value.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return "";  // If no matching key is found
    }
   
    
}

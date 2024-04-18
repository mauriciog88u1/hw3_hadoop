package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class q7Reducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, String> songArtistMap = new HashMap<>();
    private Map<String, List<Double>> startTime = new HashMap<>();
    private Map<String, List<Double>> pitches = new HashMap<>();
    private Map<String, List<Double>> timbre = new HashMap<>();
    private Map<String, List<Double>> maxLoudness = new HashMap<>();
    private Map<String, List<Double>> maxLoudnessTime = new HashMap<>();
    private Map<String, List<Double>> startLoudness = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(":", 2);
            if (parts.length > 1) {
                String songId = key.toString();
                if (parts[0].equals("title")) {
                    songArtistMap.put(songId, parts[1]);
                } else {
                    List<Double> dataList = getListForType(parts[0], songId);
                    if (dataList != null) {
                        try {
                            String[] dataParts = parts[1].replaceAll("[\\[\\]]", "").split(" ");
                            for (String part : dataParts) {
                                if (!part.isEmpty()) {
                                    dataList.add(Double.parseDouble(part.trim()));
                                }
                            }
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid double format in input: " + parts[1]);
                        }
                    }
                }
            }
        }
    }

    private List<Double> getListForType(String type, String songId) {
        switch (type) {
            case "StartTime": return startTime.computeIfAbsent(songId, k -> new ArrayList<>());
            case "Pitches": return pitches.computeIfAbsent(songId, k -> new ArrayList<>());
            case "Timbre": return timbre.computeIfAbsent(songId, k -> new ArrayList<>());
            case "MaxLoudness": return maxLoudness.computeIfAbsent(songId, k -> new ArrayList<>());
            case "MaxLoudnessTime": return maxLoudnessTime.computeIfAbsent(songId, k -> new ArrayList<>());
            case "StartLoudness": return startLoudness.computeIfAbsent(songId, k -> new ArrayList<>());
            default:
                System.err.println("Ignoring unknown type: " + type);
                return null;  // Return null to indicate that this type should be ignored.
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String songId : songArtistMap.keySet()) {
            double avgStartTime = calculateAverage(startTime.get(songId));
            double avgPitches = calculateAverage(pitches.get(songId));
            double avgTimbre = calculateAverage(timbre.get(songId));
            double avgMaxLoudness = calculateAverage(maxLoudness.get(songId));
            double avgMaxLoudnessTime = calculateAverage(maxLoudnessTime.get(songId));
            double avgStartLoudness = calculateAverage(startLoudness.get(songId));

            String result = String.format("%s Avg Start time %.2f AvgPitches %.2f avgTimbre %.2f AvgMaxLoudness%.2f AvgMaxLoudnessTime%.2f AvgStartLoudness %.2f", 
                songArtistMap.get(songId), avgStartTime, avgPitches, avgTimbre, avgMaxLoudness, avgMaxLoudnessTime, avgStartLoudness);

            context.write(new Text(songId), new Text(result));
        }
    }

    private double calculateAverage(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (double value : values) {
            sum += value;
        }
        return sum / values.size();
    }
}

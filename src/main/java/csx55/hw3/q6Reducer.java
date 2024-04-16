package csx55.hw3;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;

public class q6Reducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, String> titleMap = new HashMap<>();
    private Map<String, Double> attributesMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String id = key.toString();
        String title = "";
        double danceability = 0.0;
        double energy = 0.0;

        for (Text val : values) {
            String valStr = val.toString();
            if (valStr.startsWith("title:")) {
                title = valStr.substring(6);  
            } else {
                String[] parts = valStr.split("\\|");
                danceability = Double.parseDouble(parts[0].split(":")[1]);
                energy = Double.parseDouble(parts[1].split(":")[1]);
            }
        }

        titleMap.put(id, title);
        attributesMap.put(id, danceability + energy);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        TreeMap<String, Double> sortedMap = new TreeMap<>(Comparator.comparingDouble(attributesMap::get).reversed());
        sortedMap.putAll(attributesMap);

        int count = 0;
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            if (count++ < 10) {
                String songID = entry.getKey();
                String songTitle = titleMap.get(songID);
                Double combinedValue = entry.getValue();
                context.write(new Text(songID), new Text("title: " + songTitle + ", combinedValue: " + combinedValue));
            } else {
                break;  
            }
        }
    }
}

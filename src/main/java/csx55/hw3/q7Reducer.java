package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class q7Reducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, List<Double>> categoryAverages = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(":", 2);
            if (parts.length > 1) {
                String category = parts[0];
                List<Double> categoryValues = categoryAverages.computeIfAbsent(category, k -> new ArrayList<>());

                try {
                    String[] dataParts = parts[1].replaceAll("[\\[\\]]", "").split(" ");
                    for (String part : dataParts) {
                        if (!part.isEmpty()) {
                            categoryValues.add(Double.parseDouble(part.trim()));
                        }
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Invalid double format in input: " + parts[1]);
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String category : categoryAverages.keySet()) {
            double avg = calculateAverage(categoryAverages.get(category));
            context.write(new Text(category), new Text(String.format("Average %s: %.2f \n", category, avg)));
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

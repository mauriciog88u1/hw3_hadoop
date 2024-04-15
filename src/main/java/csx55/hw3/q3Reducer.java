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
            try{
            String[] parts = value.toString().split(":");
                if (parts[0].equals("title")) {
                    songTitles.put(key.toString(), parts[1]);
                } else if (parts[0].equals("hotness")) {
                    double hotness = Double.parseDouble(parts[1]);
                    if (hotness > maxScore) {
                        maxScore = hotness;
                        hottestSongId = key.toString();
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
                songTitles.put(key.toString(), "N/A");
                continue;
            }
        }
        
        
    }
    

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(songTitles.get(hottestSongId)), new Text(String.format("Highest Hotness: %.2f", maxScore)));
    }
}

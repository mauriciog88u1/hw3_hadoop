package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q3Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String maxSong = "";
        double maxHotttnesss = 0.0;
        for (Text value : values) {
            String[] data = value.toString().split("\\|");
            String song = data[0];
            String hotttnesssStr = data[1];
            if (!"nan".equals(hotttnesssStr)) {
                double hotttnesss = Double.parseDouble(hotttnesssStr);
                if (hotttnesss > maxHotttnesss) {
                    maxHotttnesss = hotttnesss;
                    maxSong = song;
                }
            }
        }
        context.write(key, new Text(maxSong + "|" + maxHotttnesss));
    }
}
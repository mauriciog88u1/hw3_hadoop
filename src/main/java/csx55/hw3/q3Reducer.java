package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q3Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String songName = "";
        double maxHotttnesss = 0.0;
        for (Text value : values) {
            String[] data = value.toString().split("\\|");
            if (data[0].equals("A")) {
                double hotttnesss = Double.parseDouble(data[1]);
                if (hotttnesss > maxHotttnesss) {
                    maxHotttnesss = hotttnesss;
                }
            } else if (data[0].equals("M")) {
                songName = data[1];
            }
        }
        if (!songName.isEmpty()) {
            context.write(new Text(songName), new Text(String.valueOf(maxHotttnesss)));
        }
    }
}
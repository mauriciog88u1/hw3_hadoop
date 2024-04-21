package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q9Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalTempo = 0, totalDuration = 0, totalDanceability = 0, totalEnergy = 0, totalLoudness = 0;
        double totalTimeSignature = 0, totalMode = 0, totalKey = 0, totalFadeInEnd = 0, totalFadeOutStart = 0;
        int count = 0;

        for (Text value : values) {
            String[] attributeParts = value.toString().split(",");
            for (String attribute : attributeParts) {
                String[] keyValue = attribute.split(":");
                double attrValue = Double.parseDouble(keyValue[1]);
                switch (keyValue[0]) {
                    case "tempo":
                        totalTempo += attrValue;
                        break;
                    case "duration":
                        totalDuration += attrValue;
                        break;
                    case "danceability":
                        totalDanceability += attrValue;
                        break;
                    case "energy":
                        totalEnergy += attrValue;
                        break;
                    case "loudness":
                        totalLoudness += attrValue;
                        break;
                    case "timeSig":
                        totalTimeSignature += attrValue;
                        break;
                    case "mode":
                        totalMode += attrValue;
                        break;
                    case "key":
                        totalKey += attrValue;
                        break;
                    case "fadeInEnd":
                        totalFadeInEnd += attrValue;
                        break;
                    case "fadeOutStart":
                        totalFadeOutStart += attrValue;
                        break;
                }
            }
            count++;
        }

        if (count > 0) {
            context.write(new Text("Average Tempo"), new Text(String.format("%.2f", totalTempo / count)));
            context.write(new Text("Average Duration"), new Text(String.format("%.2f", totalDuration / count)));
            context.write(new Text("Average Danceability"), new Text(String.format("%.2f", totalDanceability / count)));
            context.write(new Text("Average Energy"), new Text(String.format("%.2f", totalEnergy / count)));
            context.write(new Text("Average Loudness"), new Text(String.format("%.2f", totalLoudness / count)));
            context.write(new Text("Average Time Signature"), new Text(String.format("%.2f", totalTimeSignature / count)));
            context.write(new Text("Average Mode"), new Text(String.format("%.2f", totalMode / count)));
            context.write(new Text("Average Key"), new Text(String.format("%.2f", totalKey / count)));
            context.write(new Text("Average Fade In End Time"), new Text(String.format("%.2f", totalFadeInEnd / count)));
            context.write(new Text("Average Fade Out Start Time"), new Text(String.format("%.2f", totalFadeOutStart / count)));
        }
    }
}

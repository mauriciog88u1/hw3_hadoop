package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongReducer extends Reducer<Text, Text, Text, Text> {
    private Text maxArtistQ1 = new Text();
    private int maxCountQ1 = 0;

    private float maxLoudnessQ2 = Float.MIN_VALUE;
    private String loudestArtistQ2 = "";
    private String loudestSongIDQ2 = "";
    private int questionNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.questionNumber = context.getConfiguration().getInt("question.number", 1);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (questionNumber == 1) {
            processQuestion1(key, values);
        } else if (questionNumber == 2) {
            processQuestion2(key, values);
        }
    }

    private void processQuestion1(Text key, Iterable<Text> values) {
        int sum = 0;
        for (Text val : values) {
            try {
                sum += Integer.parseInt(val.toString());
            } catch (NumberFormatException e) {
                // Ignore non-integer values
            }
        }
        if (sum > maxCountQ1) {
            maxCountQ1 = sum;
            maxArtistQ1.set(key);
        }
    }

    private void processQuestion2(Text key, Iterable<Text> values) {
        float currentLoudness = 0;
        String artistName = null;

        for (Text value : values) {
            String strValue = value.toString();
            System.out.println("Key: " + key.toString() + ", Value: " + strValue); // Debug statement
            try {
                currentLoudness = Float.parseFloat(strValue);
                if (currentLoudness > maxLoudnessQ2) {
                    maxLoudnessQ2 = currentLoudness;
                    loudestSongIDQ2 = key.toString();
                }
            } catch (NumberFormatException e) {
                artistName = strValue;
                if (key.toString().equals(loudestSongIDQ2)) {
                    loudestArtistQ2 = artistName;
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (questionNumber == 1) {
            context.write(maxArtistQ1, new Text(String.valueOf(maxCountQ1)));
        } else if (questionNumber == 2) {
            context.write(new Text(loudestSongIDQ2), new Text(loudestArtistQ2));
        }
    }
}

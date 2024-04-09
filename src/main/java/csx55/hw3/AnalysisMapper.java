package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.AnalysisData.LOUDNESS_INDEX;
import static csx55.hw3.AnalysisData.SONG_ID_INDEX_ANALYSIS;

public class AnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text songID = new Text();
    private final Text loudestSong = new Text();
    private int questionNumber;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.questionNumber = context.getConfiguration().getInt("question.number", 1);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\\|");

        if (data.length > 0) {
            switch (questionNumber) {
                case 2:
                    processQuestion2(data, context);
                    break;
                default:
                    break;
            }
        }
    }

    private void processQuestion2(String[] data, Context context) throws IOException, InterruptedException {
        
        IntWritable loudness = new IntWritable();
        int loudestSong = Integer.parseInt(data[LOUDNESS_INDEX]);
        loudness.set(loudestSong);
        songID.set(data[SONG_ID_INDEX_ANALYSIS]);
        context.write(songID, loudness);
    }
   

}

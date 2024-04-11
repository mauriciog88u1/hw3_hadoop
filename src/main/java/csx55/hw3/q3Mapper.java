package csx55.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.utils.Analysis.SONG_HOTTTNESSS_INDEX;
import static csx55.hw3.utils.Analysis.SONG_ID_INDEX_ANALYSIS;


public class q3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\\|");
        String song = data[SONG_ID_INDEX_ANALYSIS];
        String hotttnesss = data[SONG_HOTTTNESSS_INDEX];
        context.write(new Text("Hotttnesss"), new Text(song + "|" + hotttnesss));
    }


}
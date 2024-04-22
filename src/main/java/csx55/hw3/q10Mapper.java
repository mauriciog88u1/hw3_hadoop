package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static csx55.hw3.utils.metadata.*;

public class q10Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length > YEAR_INDEX && parts.length > ARTIST_TERMS_INDEX && parts.length > ARTIST_LOCATION_INDEX) {
            String year = parts[YEAR_INDEX].trim();
            String genre = parts[ARTIST_TERMS_INDEX].trim().toLowerCase();
            String location = parts[ARTIST_LOCATION_INDEX].trim().toLowerCase();
            if ((genre.contains("cumbia") || genre.contains("reggaeton") || genre.contains("latin") || genre.contains("musica") 
            || genre.contains("reggae") || genre.contains("salsa") || genre.contains("bachata") || genre.contains("merengue")) &&
            (location.contains("mexico") || location.contains("colombia") || location.contains("puerto rico") || location.contains("cuba") ||
            location.contains("dominican republic") || location.contains("venezuela") || location.contains("argentina") || location.contains("chile") ||
            location.contains("peru") || location.contains("ecuador") || location.contains("guatemala") || location.contains("honduras") || location.contains("paraguay") ||
            location.contains("el salvador") || location.contains("nicaragua") || location.contains("costa rica") || location.contains("panama") || location.contains("bolivia") ||
            location.contains("uruguay"))) {

                word.set(year + "_" + location + "_" + genre);
                context.write(word, one);
            }
        }
    }
}

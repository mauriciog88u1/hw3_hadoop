package csx55.hw3;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import static csx55.hw3.utils.metadata.*;

import java.io.IOException;

public class q8Mapper extends Mapper<Object, Text, Text, IntWritable> {

    IntWritable a = new IntWritable();



    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{

        String parts[] = value.toString().split("\\|");

        if(parts.length > ARTIST_TERMS_INDEX || parts.length > SIMILAR_ARTISTS_INDEX){
            String artistName = parts[ARTIST_NAME_INDEX];
            int similarArtistCount = parts[SIMILAR_ARTISTS_INDEX].split(" ").length;
            int artistTermsCount = parts[ARTIST_TERMS_INDEX].split(" ").length;
            a.set(similarArtistCount + artistTermsCount);
            context.write(new Text(artistName), a);
        }
    
        
    }


}

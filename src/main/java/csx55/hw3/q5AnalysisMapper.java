package csx55.hw3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import static csx55.hw3.utils.Analysis.*;


public class q5AnalysisMapper extends Mapper<Object, Text, Text, Text> {

/*
 * q5 is   What is the longest song(s)? The shortest song(s)? The song(s) of median length?
 */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String [] parts = value.toString().split("\\|");

        try {
            if(parts.length > DURATION_INDEX && parts.length > SONG_ID_INDEX_ANALYSIS ){
                String duratiion = parts[DURATION_INDEX].equalsIgnoreCase("nan") ? "0" : parts[DURATION_INDEX];
                context.write(new Text(parts[SONG_ID_INDEX_ANALYSIS]),new Text( "duration:"+duratiion));
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            
        }
        
        
    }
    
}

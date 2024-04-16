package csx55.hw3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import static csx55.hw3.utils.Analysis.*;

import java.io.IOException;

public class q6AnalysisMapper extends Mapper<Object, Text, Text, Text> {



    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
           String [] parts = value.toString().split("\\|");
              if(parts.length > SONG_ID_INDEX_ANALYSIS && parts.length > DANCEABILITY_INDEX && parts.length > ENERGY_INDEX){
                String danceability = parts[DANCEABILITY_INDEX].equalsIgnoreCase("nan") ? "0" : parts[DANCEABILITY_INDEX];
                String energy = parts[ENERGY_INDEX].equalsIgnoreCase("nan") ? "0" : parts[ENERGY_INDEX];
                context.write(new Text(parts[SONG_ID_INDEX_ANALYSIS]),new Text("danceability:"+danceability+"|energy:"+energy));
              }
          
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





}

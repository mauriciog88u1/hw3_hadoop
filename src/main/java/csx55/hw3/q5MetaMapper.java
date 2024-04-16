package csx55.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import static csx55.hw3.utils.metadata.*;

public class q5MetaMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) {

        String[] parts = value.toString().split("\\|");

        try {
            if (parts.length > TITLE_INDEX) {
                
                String songId = parts[SONG_ID_INDEX];
                String artistName = parts[TITLE_INDEX];
                context.write(new Text(songId), new Text("title:" + artistName));
            }
        } catch (Exception e) {
            System.out.println("Error in q4MetaMapper: " + e.getMessage());
        }

    }

}

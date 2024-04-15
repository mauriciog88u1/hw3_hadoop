package csx55.hw3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;


public class q4Reducer extends Reducer<Text, Text, Text, Text>{


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
    }
        
        
    
    
}
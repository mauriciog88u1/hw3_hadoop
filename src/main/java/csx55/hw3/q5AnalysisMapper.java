package csx55.hw3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class q5AnalysisMapper extends Mapper<Object, Text, Text, Text> {


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
    }
    
}

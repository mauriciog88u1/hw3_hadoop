package csx55.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q10Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get(); // Sum all counts for the current key
        }
        result.set(sum);

        // Format the key by replacing spaces and other characters to make it suitable for CSV format
        String csvFormattedKey = key.toString().replace(" ", "_").replace("'", "").replace(",", ";");
        context.write(new Text(csvFormattedKey), result); // Write the formatted key and the sum to the context
    }
}

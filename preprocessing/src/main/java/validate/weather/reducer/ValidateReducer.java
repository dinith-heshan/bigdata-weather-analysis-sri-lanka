package validate.weather.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ValidateReducer extends Reducer<Text, Text, Text, IntWritable> {

    private final IntWritable outVal = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {

        int sum = 0;
        for (Text val : values) {
            // each mapper emitted "1" as Text, so parse it to integer and sum
            sum += Integer.parseInt(val.toString());
        }

        outVal.set(sum);
        context.write(key, outVal);  // emit key (error type or VALID) and total count
    }
}

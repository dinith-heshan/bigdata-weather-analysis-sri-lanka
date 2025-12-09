package analyse.weather.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class AnalyseMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (key.get() == 0 && line.toLowerCase().startsWith("location_id")) {
            return;
        }
        String[] f = line.split(",", -1);

        String date = f[1].trim();
        String precipitation_hours = f[13].trim();

        String month = date.split("/")[0];
        String year = date.split("/")[2];

        Text outKey = new Text(month + "-" + year);
        Text outVal = new Text(precipitation_hours);

        context.write(outKey, outVal);
    }
}

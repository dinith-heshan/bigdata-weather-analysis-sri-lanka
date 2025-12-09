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

    private final Map<String, String> locMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null) {
            for (URI uri : cacheFiles) {
                String path = uri.getPath();
                if (path.endsWith("locationData.csv")) {
                    try (BufferedReader br = new BufferedReader(new FileReader("locationData.csv"))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            if (line.toLowerCase().startsWith("location_id")) continue;
                            String[] f = line.split(",", -1);
                            String location_id = f[0].trim();
                            String location = f[7].trim();
                            locMap.put(location_id, location);
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (key.get() == 0 && line.toLowerCase().startsWith("location_id")) {
            return;
        }
        String[] f = line.split(",", -1);

        String location_id = f[0].trim();
        String date = f[1].trim();
        String temperature_2m_mean = f[5].trim();
        String precipitation_hours = f[13].trim();

        String location = locMap.getOrDefault(location_id, "UNKNOWN");
        String month = date.split("/")[0];

        Text outKey = new Text(location + "-" + month);
        Text outVal = new Text(temperature_2m_mean + "," + precipitation_hours);

        context.write(outKey, outVal);
    }
}

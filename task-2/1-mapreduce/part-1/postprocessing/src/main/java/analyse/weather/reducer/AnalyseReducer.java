package analyse.weather.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnalyseReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {  
                      
        double sum_temperature_2m_mean = 0.0;
        int sum_precipitation_hours = 0;
        int val_count = 0;

        for (Text val : values) {
            double temperature_2m_mean = Double.parseDouble(val.toString().split(",", -1)[0]);
            int precipitation_hours = Integer.parseInt(val.toString().split(",", -1)[1]);

            sum_temperature_2m_mean += temperature_2m_mean;
            sum_precipitation_hours += precipitation_hours;

            val_count++;
        }

        double avg_temperature_2m_mean = sum_temperature_2m_mean/val_count;

        String location = key.toString().split("-", -1)[0];
        int month = Integer.parseInt(key.toString().split("-", -1)[1]);

        String monthOrdinal = getMonthOrdinal(month);

        String result = String.format(
            "%s had a total precipitation of %d hours with a mean temperature of %.1f for %s month",
            location, sum_precipitation_hours, avg_temperature_2m_mean, monthOrdinal
        );

        outVal.set(result);
        context.write(null, outVal);
    }

    private String getMonthOrdinal(int month) {
        if (month > 3) {
            return month + "th";
        }
        switch (month) {
            case 1: return month + "st";
            case 2: return month + "nd";
            case 3: return month + "rd";
            default: return month + "th";
        }
    }
}

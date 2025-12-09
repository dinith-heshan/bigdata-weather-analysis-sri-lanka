package analyse.weather.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnalyseReducer extends Reducer<Text, Text, Text, Text> {

    private int max_sum_precipitation_hours = 0;
    private String max_month_year = "";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {  
                      
        int sum_precipitation_hours = 0;

        for (Text val : values) {
            int precipitation_hours = Integer.parseInt(val.toString().split(",", -1)[1]);

            sum_precipitation_hours += precipitation_hours;
        }

        if (sum_precipitation_hours > max_sum_precipitation_hours) {
            max_sum_precipitation_hours = sum_precipitation_hours;
            max_month_year = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
                
        int month = Integer.parseInt(max_month_year.toString().split("-", -1)[0]);
        String year = max_month_year.toString().split("-", -1)[1];

        String monthOrdinal = getMonthOrdinal(month);
        
        String result = String.format( 
            "%s month in %s had the total precipitation of %d hr",
            monthOrdinal, year, max_sum_precipitation_hours
        );

        context.write(new Text(max_month_year), new Text(result));
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

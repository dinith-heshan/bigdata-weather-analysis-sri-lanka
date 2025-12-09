package validate.weather.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import java.time.LocalTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class ValidateMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("H:mm");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("M/d/yyyy");

    // Keys emitted: "VALID" or specific error tags like "ERR_location_id_range", etc.
    private final Text outKey = new Text();
    private final Text outVal = new Text("1"); // count = 1

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // skip header (assumes header starts with 'location_id' or is first line)
        if (key.get() == 0 && line.toLowerCase().startsWith("location_id")) {
            return;
        }

        // keep empty tokens
        String[] f = line.split(",", -1);

        if (f.length != 21) {
            outKey.set("ERR_field_count");
            context.write(outKey, outVal);
            return;
        }

        // Validate fields step by step; on any failure emit the specific error and return.
        // Field idx mapping per your confirmed order:
        // 0 location_id, 1 date, 2 weather_code, 3..8 temps, 9 daylight_duration, 10 sunshine_duration,
        // 11 precipitation_sum,12 rain_sum,13 precipitation_hours,14 wind_speed_10m_max,15 wind_gusts_10m_max,
        // 16 wind_direction_10m_dominant,17 shortwave_radiation_sum,18 et0_fao_evapotranspiration,
        // 19 sunrise,20 sunset

        // location_id
        String s;
        try {
            s = f[0].trim();
            if (s.isEmpty()) { emitErr("ERR_location_id_null", context); return; }
            int loc = Integer.parseInt(s);
            if (loc < 0 || loc > 26) { emitErr("ERR_location_id_range", context); return; }
        } catch (NumberFormatException e) { emitErr("ERR_location_id_type", context); return; }

        // date
        s = f[1].trim();
        if (s.isEmpty()) { emitErr("ERR_date_null", context); return; }
        try {
            LocalDate.parse(s, DATE_FORMATTER);
        } catch (DateTimeParseException e) { emitErr("ERR_date_invalid", context); return; }

        // weather_code
        try {
            s = f[2].trim();
            if (s.isEmpty()) { emitErr("ERR_weather_code_null", context); return; }
            int wc = Integer.parseInt(s);
            if (wc < 0 || wc > 99) { emitErr("ERR_weather_code_range", context); return; }
        } catch (NumberFormatException e) { emitErr("ERR_weather_code_type", context); return; }

        // temperature fields (3..8) -> double, -100..100
        for (int i = 3; i <= 8; i++) {
            s = f[i].trim();
            if (s.isEmpty()) { emitErr("ERR_temp_null_idx_" + i, context); return; }
            try {
                double d = Double.parseDouble(s);
                if (d < -100.0 || d > 100.0) { emitErr("ERR_temp_range_idx_" + i, context); return; }
            } catch (NumberFormatException e) { emitErr("ERR_temp_type_idx_" + i, context); return; }
        }

        // daylight_duration (9) and sunshine_duration (10): double 0..86400
        for (int i : new int[]{9, 10}) {
            s = f[i].trim();
            if (s.isEmpty()) { emitErr("ERR_duration_null_idx_" + i, context); return; }
            try {
                double d = Double.parseDouble(s);
                if (d < 0.0 || d > 86400.0) { emitErr("ERR_duration_range_idx_" + i, context); return; }
            } catch (NumberFormatException e) { emitErr("ERR_duration_type_idx_" + i, context); return; }
        }

        // precipitation_sum (11), rain_sum (12): double >= 0
        for (int i : new int[]{11, 12}) {
            s = f[i].trim();
            if (s.isEmpty()) { emitErr("ERR_precip_null_idx_" + i, context); return; }
            try {
                double d = Double.parseDouble(s);
                if (d < 0.0) { emitErr("ERR_precip_range_idx_" + i, context); return; }
            } catch (NumberFormatException e) { emitErr("ERR_precip_type_idx_" + i, context); return; }
        }

        // precipitation_hours (13): int 0..24
        try {
            s = f[13].trim();
            if (s.isEmpty()) { emitErr("ERR_precip_hours_null", context); return; }
            int ph = Integer.parseInt(s);
            if (ph < 0 || ph > 24) { emitErr("ERR_precip_hours_range", context); return; }
        } catch (NumberFormatException e) { emitErr("ERR_precip_hours_type", context); return; }

        // wind_speed_10m_max (14), wind_gusts_10m_max (15): double >= 0
        for (int i : new int[]{14, 15}) {
            s = f[i].trim();
            if (s.isEmpty()) { emitErr("ERR_wind_null_idx_" + i, context); return; }
            try {
                double d = Double.parseDouble(s);
                if (d < 0.0) { emitErr("ERR_wind_range_idx_" + i, context); return; }
            } catch (NumberFormatException e) { emitErr("ERR_wind_type_idx_" + i, context); return; }
        }

        // wind_direction_10m_dominant (16): int 0..360
        try {
            s = f[16].trim();
            if (s.isEmpty()) { emitErr("ERR_wind_dir_null", context); return; }
            int wd = Integer.parseInt(s);
            if (wd < 0 || wd > 360) { emitErr("ERR_wind_dir_range", context); return; }
        } catch (NumberFormatException e) { emitErr("ERR_wind_dir_type", context); return; }

        // shortwave_radiation_sum (17), et0_fao_evapotranspiration (18): double >=0
        for (int i : new int[]{17, 18}) {
            s = f[i].trim();
            if (s.isEmpty()) { emitErr("ERR_rad_null_idx_" + i, context); return; }
            try {
                double d = Double.parseDouble(s);
                if (d < 0.0) { emitErr("ERR_rad_range_idx_" + i, context); return; }
            } catch (NumberFormatException e) { emitErr("ERR_rad_type_idx_" + i, context); return; }
        }

        // sunrise (19) and sunset (20): HH:MM 24h
        for (int i : new int[]{19, 20}) {
            s = f[i].trim();
            if (s.isEmpty()) { emitErr("ERR_time_null_idx_" + i, context); return; }
            try {
                LocalTime.parse(s, TIME_FORMATTER);
            } catch (DateTimeParseException e) { emitErr("ERR_time_format_idx_" + i, context); return; }
        }

        // all checks passed
        outKey.set("VALID");
        context.write(outKey, outVal);
    }

    private void emitErr(String tag, Context context) throws IOException, InterruptedException {
        outKey.set(tag);
        context.write(outKey, outVal);
    }
}

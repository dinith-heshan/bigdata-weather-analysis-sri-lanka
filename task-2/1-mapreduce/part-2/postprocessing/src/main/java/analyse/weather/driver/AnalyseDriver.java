package analyse.weather.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import analyse.weather.mapper.AnalyseMapper;
import analyse.weather.reducer.AnalyseReducer;

public class AnalyseDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: AnalyseDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Analysis");
        job.setJarByClass(AnalyseDriver.class);

        // Mapper & Reducer
        job.setMapperClass(AnalyseMapper.class);
        job.setReducerClass(AnalyseReducer.class);

        // Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input / Output paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // weather CSV
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

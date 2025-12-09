package validate.weather.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import validate.weather.mapper.ValidateMapper;
import validate.weather.reducer.ValidateReducer;

public class ValidateDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: ValidateDriver <input-path> <output-path>");
            System.exit(2);
        }

        // Hadoop configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Validate Weather Data CSV Schema");

        // Set the driver class (for Hadoop to locate jar)
        job.setJarByClass(ValidateDriver.class);

        // Set mapper and reducer classes
        job.setMapperClass(ValidateMapper.class);
        job.setReducerClass(ValidateReducer.class);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Reducer output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and output paths
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        // Input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Run the job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

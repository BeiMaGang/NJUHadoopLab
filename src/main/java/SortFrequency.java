import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class SortFrequency {

    public static class SortMapper extends
            Mapper<Object, Text, FloatWritable, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().split(",")[0];
            String token = line.split("[\t]")[0];
            String freq = line.split("[\t]")[1];
            context.write(new FloatWritable(Float.valueOf(freq)),new Text(token));
        }
    }

    public static class SortReduce extends
            Reducer<FloatWritable, Text, Text, Text> {

        public void reduce(FloatWritable key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            String freq = key.toString();
            for (Text val : values) {
                context.write(val, new Text(freq));
            }
        }
    }


    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Sort According to Frequency");
        job.setJarByClass(SortFrequency.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

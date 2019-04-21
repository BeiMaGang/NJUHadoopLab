import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * @class name:TFIDF
 * @author:Wu Gang
 * @create: 2019-04-21 15:10
 * @description:
 */
public class TFIDF {

    public static class TFIDFMapper extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName().split("\\.")[0];
            String author = fileName.split("[0-9]+")[0];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                Text sendKey = new Text();
                sendKey.set(author + "#"  + itr.nextToken() + "#" + fileName);
                context.write(sendKey, new IntWritable(1));
            }
        }
    }

    public static class TFIDFPartitioner extends HashPartitioner<Text, IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String splitKey = key.toString().split("#")[1];
            return super.getPartition(new Text(splitKey), value, numReduceTasks);
        }
    }

    public static class TFIDFReducer extends Reducer<Text, IntWritable, Text, Text>{
        private String curWord = "";
        private String curAuthor = "";
        private String curDoc = "";
        private int totalDocNum;

        private int sumDoc;
        private int sumWord;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalDocNum = Integer.parseInt(context.getConfiguration().get("totalDocNum"));
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String author = key.toString().split("#")[0];
            String word = key.toString().split("#")[1];
            String doc = key.toString().split("#")[2];
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            if (!curWord.equals(word) && !curWord.isEmpty()||
                    !curAuthor.equals(author) && !curAuthor.isEmpty()){
                cleanup(context);
            }
            curWord = word;
            curAuthor = author;
            curDoc = doc;
            sumDoc++;
            sumWord += sum;
            super.reduce(key, values, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int tf = sumWord;
            double idf = Math.log(totalDocNum * 1.0/ (sumDoc + 1));
            context.write(new Text(curAuthor + ", " + curWord + ','),
                    new Text("tf:" + tf + " idf:" + idf));
            sumWord = 0;
            sumDoc = 0;
            curDoc = "";
            curAuthor = "";
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus fileNames[] = hdfs.listStatus(new Path(args[0]));
        conf.set("totalDocNum", String.valueOf(fileNames.length));

        Job job = Job.getInstance(conf, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDF.TFIDFMapper.class);
        job.setPartitionerClass(TFIDF.TFIDFPartitioner.class);
        job.setReducerClass(TFIDF.TFIDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

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
import java.util.ArrayList;
import java.util.List;
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
                sendKey.set( itr.nextToken() + "#" + author  + "#" + fileName);
                context.write(sendKey, new IntWritable(1));
            }
        }
    }

    public static class TFIDFPartitioner extends HashPartitioner<Text, IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String splitKey = key.toString().split("#")[0];
            return super.getPartition(new Text(splitKey), value, numReduceTasks);
        }
    }

    public static class TFIDFReducer extends Reducer<Text, IntWritable, Text, Text>{
        private String curWord = "";
        private String curAuthor = "";
        private int totalDocNum;

        private int sumDoc;
        private int sumWord;

        private List<String> tmp = new ArrayList<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalDocNum = Integer.parseInt(context.getConfiguration().get("totalDocNum"));
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String word = key.toString().split("#")[0];
            String author = key.toString().split("#")[1];
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            if (!curAuthor.equals(author) && !curAuthor.isEmpty()|| !curWord.equals(word) && !curWord.isEmpty()){
                authorChanged();
            }
            if (!curWord.equals(word) && !curWord.isEmpty()){
                wordChanged(context);
            }
            curWord = word;
            curAuthor = author;
            sumDoc++;
            sumWord += sum;
        }

        private void authorChanged(){
            int tf = sumWord;
            String info = curAuthor + ", " + curWord + ", " + "tf:" + tf + ", ";
            tmp.add(info);
            sumWord = 0;
        }

        private void wordChanged(Context context) throws IOException, InterruptedException {
            double idf = Math.log(totalDocNum * 1.0/ (sumDoc + 1));
            for(String str: tmp){
                context.write(new Text(str + "idf:" + idf), new Text());
            }
            tmp.clear();
            sumDoc = 0;
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            authorChanged();
            wordChanged(context);
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/*
 * @class name:HBase
 * @author:Wu Gang
 * @create: 2019-05-08 09:41
 * @description:
 */
public class HBaseWriter {
    public static class InvertedIndexMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            fileName = fileName.substring(0,fileName.indexOf("."));
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                this.word.set(itr.nextToken() + "#" + fileName);
                context.write(this.word, one);
            }
        }

    }

    public static class InvertedPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term;
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term),value,numReduceTasks);
        }
    }

    public static class HbaseReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp = "";
        static  Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            word1.set(key.toString().split("#")[0]);
            temp = key.toString().split("#")[1];
            for (IntWritable val : values) {
                sum += val.get();
            }
            word2.set(" " + temp + ":" + sum + ";");
            if (!CurrentItem.equals(word1) && !CurrentItem.equals(new Text(" "))) {
                cleanup(context);
                postingList = new ArrayList<>();
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder last = new StringBuilder();
            long count = 0;
            long  p = 0;
            for (String s : postingList) {
                last.append(s);
                //last.append(";");
                count += Long.parseLong(s.substring(s.indexOf(":") + 1, s.indexOf(";")));
                p ++;
            }
            double average = count / (double)p;
            if (count > 0) {
                Put put = new Put(Bytes.toBytes(CurrentItem.toString()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("word"), Bytes.toBytes(CurrentItem.toString()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("freq"), Bytes.toBytes(average));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(CurrentItem.toString())), put);
            }
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage:Merge and duplicate removal <in>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "HBase");
        job.setJarByClass(HBaseWriter.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setPartitionerClass(HBaseWriter.InvertedPartitioner.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);

        job.setNumReduceTasks(1);
        TableMapReduceUtil.initTableReducerJob("Wuxia", HbaseReduce.class, job);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

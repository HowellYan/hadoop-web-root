package examples;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static void main(String[] args) throws Exception {
//1.configuration
        Configuration conf = new Configuration();

        String[] otherArgs = new String[2];
        otherArgs[0] = "hdfs://127.0.0.1:9000/profile";
        otherArgs[1] = "hdfs://127.0.0.1:9000/out4";
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

//2.建立job
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

//5.map
        job.setMapperClass(WordCount.TokenizerMapper.class);

        /**指定本job使用combiner组件，组件所用的类为IntSumReducer**/
        job.setCombinerClass(WordCount.IntSumReducer.class);

//6.reduce
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            //3.输入文件
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            //4.格式化输入文件
            //  job.setInputFormatClass(TextInputFormat.class);
        }
        //7.输出文件
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        //8.输出文件格式化
        // job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //9.提交给集群执行
        //job.waitForCompletion(true);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                System.out.println(word.toString());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopWebTools {
    /**
     * 向HDFS上传本地文件
     * @param localFile
     * @throws IOException
     */
    public static void uploadInputFile(String localFile) throws IOException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://localhost:9000/";
        String hdfsInput = "hdfs://localhost:9000/profile";
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(localFile), new Path(hdfsInput));
        fs.close();
        System.out.println("已经上传文件到input文件夹啦");
    }

    /**
     * 将output文件下载到本地
     * @param outputfile
     * @throws IOException
     */
    public static void getOutput(String outputfile) throws IOException{
        String remoteFile = "hdfs://localhost:9000/user/hadoop/output/part-r-00000";
        Path path = new Path(remoteFile);
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://localhost:9000/";
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
        fs.copyToLocalFile(path, new Path(outputfile));
        System.out.println("已经将输出文件保留到本地文件");
        fs.close();
    }

    /**
     * 删除hdfs中的文件
     * @throws IOException
     */
    public static void deleteOutput() throws IOException{
        Configuration conf = new Configuration();
        String hdfsOutput = "hdfs://localhost:9000/out2";
        String hdfsPath = "hdfs://localhost:9000/";
        Path path = new Path(hdfsOutput);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        fs.close();
        System.out.println("output文件已经删除");
    }

    /**
     * 创建Mapper类和Reducer类
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            line = line.replace("\\", "");
            String regex = "性别：</span><span class=\"pt_detail\">(.*?)</span>";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(line);
            while(matcher.find()){
                String term = matcher.group(1);
                word.set(term);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val :values){
                sum+= val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args){
        try {
            uploadInputFile("/etc/profile");
            //deleteOutput();
        } catch (Exception e){

        }
    }

//    public static void runMapReduce(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if(otherArgs.length != 2){
//            System.err.println("Usage: wordcount<in> <out>");
//            System.exit(2);
//        }
//        Job job = new Job(conf, "word count");
//        job.setJarByClass(WordCount.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        System.out.println("mapReduce 执行完毕！");
//        System.exit(job.waitForCompletion(true)?0:1);
//    }

}

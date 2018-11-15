package SearchEngine.or;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MR {
    private static String hdfsPath = "hdfs://127.0.0.1:9000";
    private static String inPath = "/myspider/";
    private static String outPath = "/myspider/outIndex/";

    public int run() {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "index.inverted");
            job.setJarByClass(MR.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            job.setMapperClass(Map.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(Red.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path(hdfsPath + inPath));
            FileOutputFormat.setOutputPath(job, new Path(hdfsPath + outPath + System.currentTimeMillis() + "/"));

            return job.waitForCompletion(true) ? 1 : -1;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

}

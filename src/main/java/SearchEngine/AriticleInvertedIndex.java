package SearchEngine;
import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;


public class AriticleInvertedIndex {
    public static class ArticleInvertedIndexMapper extends Mapper<Text, Text, Text, Text>{

        private Text keyInfo = new Text();  // 存储单词和URI的组合
        private Text valueInfo = new Text(); //存储词频
        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            //把标题分词
            Analyzer analyzer = new IKAnalyzer(true);
            TokenStream ts = analyzer.tokenStream("field", new StringReader( value.toString().trim()  ));

            OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
            try {
                ts.reset();
                while (ts.incrementToken()) {
                    System.out.println(offsetAtt.toString().trim());
                    keyInfo.set( offsetAtt.toString()+":"+key);
                    valueInfo.set("1");
                    System.out.println("key"+keyInfo);
                    System.out.println("value"+valueInfo);
                    context.write(keyInfo, valueInfo);
                }
                ts.end();
            } finally {
                ts.close();
            }
        }
    }

    public static class ArticleInvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
        private Text info = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            //统计词频
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString() );
            }

            int splitIndex = key.toString().indexOf(":");

            //重新设置value值由URI和词频组成
            info.set( key.toString().substring( splitIndex + 1) +":"+sum );

            //重新设置key值为单词
            key.set( key.toString().substring(0,splitIndex));

            context.write(key, info);
            System.out.println("key"+key);
            System.out.println("value"+info);
        }
    }


    public static class ArticleInvertedIndexReducer extends Reducer<Text, Text, Text, Text>{

        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            //生成文档列表
            String fileList = new String();
            for (Text value : values) {
                fileList += value.toString()+";";
            }
            result.set(fileList);

            context.write(key, result);
        }

    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf,"InvertedIndex");
            job.setJarByClass(AriticleInvertedIndex.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            //实现map函数，根据输入的<key,value>对生成中间结果。
            job.setMapperClass(ArticleInvertedIndexMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setCombinerClass(ArticleInvertedIndexCombiner.class);
            job.setReducerClass(ArticleInvertedIndexReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/myspider/"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://127.0.0.1:9000/myspider_outindex"+System.currentTimeMillis()+"/"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}

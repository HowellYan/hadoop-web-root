package SearchEngine.or;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Red extends Reducer<Text, Text, Text, Text> {

    private Text OutKey = new Text();
    private Text OutValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //key:      社保
        //value:    http://news.ifeng.com/a/20171120/53406355_0.shtml?_zbs_baidu_news

        int sum = 0;
        String listString = new String();

        //统计词频
        for (Text value : values) {
            if (listString.indexOf(value.toString()) == -1) {
                listString = value.toString() + ";\t" + listString;
                sum++;
            }
        }

        OutKey.set(key + ":" + String.valueOf(sum));
        OutValue.set(listString);

        context.write(OutKey, OutValue);
    }

}

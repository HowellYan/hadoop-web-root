package SearchEngine.or;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class Map extends Mapper<Text, Text, Text, Text>{

    private Text OutKey = new Text();
    private Text OutValue = new Text();

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        //key:      http://news.ifeng.com/a/20171120/53406355_0.shtml?_zbs_baidu_news
        //value:    国资划转社保：让社保可持续的重大举措


        @SuppressWarnings("resource")
        Analyzer analyzer = new IKAnalyzer(true);
        TokenStream ts = analyzer.tokenStream("field", new StringReader( value.toString().trim()));
        CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);

        try {
            ts.reset();
            while (ts.incrementToken()) {

                OutKey.set(term.toString());
                OutValue.set(key);

                context.write(OutKey, OutValue);
            }
            ts.end();
        } finally {
            ts.close();
        }
    }

}

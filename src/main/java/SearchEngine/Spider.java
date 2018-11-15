package SearchEngine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.htmlparser.util.ParserException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

public class Spider {

    private DownLoadTool dlt = new DownLoadTool();
    private ArticleDownLoad pdl = new ArticleDownLoad();


    public static void main(String[] args) throws ParserException {
        Spider s = new Spider();
        //  s.crawling("http://blog.csdn.net");
        s.crawling("http://www.sohu.com");

    }


    public void crawling(String url) {
        try {
            Configuration conf = new Configuration();
            URI uri = new URI("hdfs://127.0.0.1:9000");
            FileSystem hdfs = FileSystem.get(uri, conf);

            String html = dlt.downLoadUrl(url);
            Set<String> allneed = pdl.getImageLink(html);
            for (String addr : allneed) {
                //生成文件路径
                Path p1 = new Path("/myspider/" + System.currentTimeMillis());
                FSDataOutputStream dos = hdfs.create(p1);
                String a = addr + "\n";
                //把内容写入文件
                dos.write(a.getBytes());
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

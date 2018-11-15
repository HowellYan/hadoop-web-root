package SearchEngine;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArticleDownLoad {
    /**
     * 取出文章的a标记href
     */
    static String ARTICLE_URL = "<a\\s+[^<>]*\\s+href=\"?(http[^<>\"]*)\"[^<>]*>([^<]*)</a>";


    static Set<String> getImageLink(String html) {
        Set<String> result = new HashSet<String>();
        // 创建一个Pattern模式类，编译这个正则表达式
        Pattern p = Pattern.compile(ARTICLE_URL, Pattern.CASE_INSENSITIVE);
        // 定义一个匹配器的类
        Matcher matcher = p.matcher(html);
        while (matcher.find()) {
            System.out.println("======="+matcher.group(1)+"\t"+matcher.group(2));
            result.add(matcher.group(1)+"\t"+matcher.group(2));
        }
        return result;

    }
}

package SearchEngine;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class DownLoadTool {
    public String downLoadUrl(final String addr) {
        StringBuffer sb = new StringBuffer();
        try {
            URL url;
            if(addr.startsWith("http://")==false){
                String urladdr=addr+"http://";
                url = new URL(urladdr);
            }else{
                System.out.println(addr);
                url = new URL(addr);
            }
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setConnectTimeout(5000);
            con.connect();
            if (con.getResponseCode() == 200) {
                BufferedInputStream bis = new BufferedInputStream(con
                        .getInputStream());
                Scanner sc = new Scanner(bis,"utf-8");
                while (sc.hasNextLine()) {
                    sb.append(sc.nextLine());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

}

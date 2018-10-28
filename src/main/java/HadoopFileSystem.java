import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HadoopFileSystem
{
    public static final String HDFS_PATH = "hdfs://127.0.0.1:9000";
    public static void main(String[] args)
    {
        readFile();
        //writeFile();
        listFile("/out");
    }

    static void listFile(String listPath)
    {
        try
        {
            FileSystem fs = FileSystem.get(new URI(HDFS_PATH), new Configuration());
            FileStatus[] files = fs.listStatus(new Path(listPath));
            for (FileStatus f : files)
            {
                if (f.isDir())
                {
                    System.out.println("d " + f.getPath());
                    listFile(f.getPath().toString());
                }
                else
                {
                    System.out.println("- " + f.getPath());
                }
            }
        }
        catch (URISyntaxException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    static void writeFile()
    {
        try
        {
            String path = "/home/hadoop/data/writefile/write.txt";
            FileSystem fs = FileSystem.get(new URI(HDFS_PATH), new Configuration());

            if ( fs.exists(new Path(path)) )
            {
                System.out.println("file already exist, delete it first...");
                fs.deleteOnExit(new Path(path));
            }

            FSDataOutputStream out = fs.create(new Path(path));
            InputStream in = new FileInputStream("/home/howell/data.txt");
            IOUtils.copyBytes(in, out, 1024, true);

            System.out.println("**********write finished*******");

            FSDataInputStream i = fs.open(new Path(path));
            IOUtils.copyBytes(i, System.out, 1024, true);

        }
        catch (URISyntaxException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    static void readFile()
    {
        try
        {
            String path = HDFS_PATH + "/profile";
            FileSystem fs = FileSystem.get(URI.create(HDFS_PATH), new Configuration());
            FSDataInputStream in = fs.open(new Path(path));
            IOUtils.copyBytes(in, System.out, 1024, true);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}

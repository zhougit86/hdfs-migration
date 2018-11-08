package dirTraversal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhou1 on 2018/11/1.
 */
public class simpleRunner implements Runnable {
    private final Configuration conf = new Configuration();
    private static String DestHdfs;
    private FileSystem fs;

    private String Path;

    {

    }

    public static void setDestHdfs(String dest){
        DestHdfs = dest;
    }

    public simpleRunner(String Number,String path) {
        try{
            this.fs = FileSystem.get(new URI(DestHdfs),conf);
        }catch (URISyntaxException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
        conf.set("ipc.server.read.threadpool.size",Number);
        Path = path;
    }

    public void run(){
        int i = 0;
        while(true){
            System.out.println(new Timestamp(Time.now()) + "before ###" + this + "###"+ i);
            try{
                FileStatus[] listStatus = fs.listStatus(new Path(Path));
                i++;
            }catch (IOException e){
                e.printStackTrace();
            }
            System.out.println(new Timestamp(Time.now()) + "after ###" + this + "###" +i);
        }


    }
}

package dirTraversal;

import dirTraversal.model.Dir;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.apache.log4j.BasicConfigurator;


import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;




/**
 * Created by zhou1 on 2018/10/26.
 * 从/开始，每遇到一个Dir就生成一个traversaler
 * 里面会记录父文件夹，子文件夹和所有的目录
 */
public class traversaler {
    private final static Configuration conf = new Configuration();
    private static String DestHdfs;
    private static FileSystem fs;
    private static taskQueue tq;
    private static sqlQueue sq;
    public FileStatus fStatus;
    // /节点的FileStatus是nil
//    private traversaler parentNode;

    public static void initDestFs(String dest, taskQueue taskQueue) throws Exception{
        DestHdfs = dest;
        fs = FileSystem.get(new URI(DestHdfs),conf);

        tq = taskQueue;
    }

    public traversaler(FileStatus fStatus){
        this.fStatus = fStatus;
//        this.parentNode = parentNode;
    }

//    public static traversaler generateTraversal(FileStatus fStatus, traversaler parentNode){
//        traversaler t = new traversaler(fStatus,parentNode);
//        if (!t.fStatus.isDirectory()){
//            return t;
//        }
//        try{
//            while (tq.size()>=80){
//                System.out.println(tq.size()+"this fails:" + t.getfStatus().getPath().toString());
//                Thread.yield();
//                TimeUnit.MILLISECONDS.sleep(500);
//            }
//            tq.put(t);
////            tq.put(new traversalTask(t));
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return t;
//    }

    private traversaler(Path selfPath){
        try{
            this.fStatus = fs.getFileStatus(selfPath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static traversaler generateInitTraversal(Path selfPath){
        traversaler t = new traversaler(selfPath);
        try{
            while (tq.size()>=80){
                System.out.println(tq.size()+"this fails:" + t.getfStatus().getPath().toString());
                Thread.yield();
                TimeUnit.MILLISECONDS.sleep(500);
            }
            tq.put(t);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        return t;
    }

    @Override
    public String toString() {
        return this.fStatus.getPath().toString();
    }

    public FileStatus getfStatus(){
        return this.fStatus;
    }

}


class testMain{
    public static void main(String[] args) throws Exception {

//        BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境。

        ExecutorService exec = Executors.newCachedThreadPool();
        taskQueue tq = new taskQueue(500*Integer.parseInt(args[2]));
        taskQueueRunner.setDestHdfs(args[0]);
        try{
            traversaler.initDestFs(args[0],tq);
//            traversalTask.initDestFs(args[0],tq);
        }catch (Exception e){
            e.printStackTrace();
        }


        traversaler rt = traversaler.generateInitTraversal(new Path(args[1]));
        taskQueueRunner.setNumT(Integer.parseInt(args[2]));
//        batisWrite.setQueue(sq,endQ);
//        exec.execute(new batisWrite());
        for (int i =0;i<Integer.parseInt(args[2]);i++){
            exec.execute(new taskQueueRunner(tq));
        }


//        System.out.println(endQ.isEmpty());
        TimeUnit.SECONDS.sleep(20000);

        exec.shutdownNow();

        while (!exec.awaitTermination(2, TimeUnit.SECONDS)) {
            System.out.println("线程池没有关闭");
        }
        System.out.println("all closed");
    }
}

//class tRun implements Runnable{
//    public void run(){
//        while (!Thread.interrupted()) {
//
//        }
//        System.out.println("haha");
//    }
//}

class testMain2{
    public static void main(String[] args) throws InterruptedException{
//        ExecutorService exec = Executors.newCachedThreadPool();
//        exec.execute(new tRun());
//        exec.shutdownNow();
//        System.out.println(Time.now());
//        System.out.println(new Timestamp(Time.now() - 2*86400000L));

        ExecutorService exec = Executors.newCachedThreadPool();
        simpleRunner.setDestHdfs(args[0]);

        ArrayList<String> ssss = new ArrayList<String>();
        ssss.add("/");
        ssss.add("/warehouse");
        ssss.add("/warehouse/dim");
        ssss.add("/warehouse/dm");
        ssss.add("/warehouse/dw");
        ssss.add("/warehouse/ods");

//        batisWrite.setQueue(sq,endQ);
//        exec.execute(new batisWrite());
        for (int i =0;i<Integer.parseInt(args[1]);i++){
            exec.execute(new simpleRunner(args[2],ssss.get(i)));
        }


//        System.out.println(endQ.isEmpty());
        TimeUnit.SECONDS.sleep(20000);

        exec.shutdownNow();

        while (!exec.awaitTermination(2, TimeUnit.SECONDS)) {
            System.out.println("线程池没有关闭");
        }
        System.out.println("all closed");
    }
}
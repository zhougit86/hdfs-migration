package production;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.Strings;
import pqtRead.TestReadWriteParquet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class getTable{
    static Configuration conf= new Configuration();
//    static final String ProdHdfs = "hdfs://10.1.53.205:8020";
    static final String ProdHdfs2 = "hdfs://10.1.53.23:8020";
    static final String ProdHdfs1 = "hdfs://10.1.53.24:8020";
    static final String ActiveHdfs;
//    static final String fuzhouHdfs = "hdfs://10.1.53.23:8020";
    static final String TbdsHdfs = "hdfs://10.216.126.151:8020";
    static final String destAppendix = "/tmp/mrzip";
    static final String taskQueue = "root.tbds";

    static {
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.framework.name","yarn");
        try {
            FileSystem fileSys = FileSystem.get(new URI(ProdHdfs1),conf);
            fileSys.listStatus(new Path(ProdHdfs1+"/"));
            conf.set("fs.defaultFS", ProdHdfs1);
        }catch (Exception e){
            System.err.println(e.getClass());
            conf.set("fs.defaultFS", ProdHdfs2);
        }
        ActiveHdfs = conf.get("fs.defaultFS");
    }

    public static void main(String[] args) throws IOException,URISyntaxException{

        //通过传入的参数作为需要同步的目录
        String location = args[0];

        // 先将重庆的temp目录删除
        FileSystem fileSys= FileSystem.get(new URI(TbdsHdfs),conf);
        if (fileSys.exists(new Path(TbdsHdfs+destAppendix+location))){
            fileSys.delete(new Path(TbdsHdfs+destAppendix+location));
        }
        fileSys.close();

        //遇到文件夹会进行递归（在reader中已实现）
//        conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");

        //同步开始的时间
        //获取当前时间，当需要保存的时候则刷新数据库
        Date startTime = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        conf.set("yonghui.startTime",sdf.format(startTime));


        //设置任务的queue和任务名
        conf.set("yonghui.hdfs",ActiveHdfs);
        conf.set("mapreduce.job.queuename",taskQueue);
        conf.set("yonghui.name",
                String.format("%s_%s_%s","compress" ,args[0],
                        sdf.format(startTime))
        );

        //获取压缩MR任务的参数
        String[] compressArg = new String[]{ActiveHdfs, location, destAppendix+location ,"gzip" };
        System.out.println(Strings.join(compressArg," "));
        try {
            int res = ToolRunner.run(conf, new TestReadWriteParquet(), compressArg);
            if (res!=0){
                deleteTempDir(location);
                System.exit(res);
            }
        } catch (Exception e) {
            //压缩过程中的报错
            e.printStackTrace();
            deleteTempDir(location);
            System.out.println("got exception");
            System.exit(3);
        }
        System.out.println("compress succeed");

        //===========================压缩完成开始拷贝============================
        Path fuzhouPath = new Path( destAppendix);
//        Path fuzhouPath = new Path(ProdHdfs + destAppendix);
        Path chongQinPath = new Path( TbdsHdfs + destAppendix);

        String shellString =  "hadoop distcp -Ddfs.replication=3 "+
                String.format("-Dmapreduce.job.queuename=%s -Dmapreduce.job.name=%s_%s_%s " ,taskQueue,"copy",args[0],sdf.format(startTime) ) +
                fuzhouPath.toString() + location + " "
                + chongQinPath.toString() +location;
        System.out.println(shellString);

        try{
//            deleteCQTempDir(location);
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            if (exitValue!=0){
                System.out.println("Distcp Failure，not clear the tmp path");
//                deleteTempDir(location);
                System.exit(4);
            }
        }catch (Exception e){
            e.printStackTrace();
//            deleteTempDir(location);
            System.exit(4);
        }

        System.exit(0);
    }

    public static void deleteTempDir(String location) {
        //删除tmp目录
        String shellString = "hadoop fs -rmr " + destAppendix+location;

        try{
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
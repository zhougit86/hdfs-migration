package hive;

import hive.TBLS.model.syncLog;
import hive.TBLS.persistence.dao.sdsMapper;
import hive.TBLS.persistence.dao.tableMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import pqtRead.TestReadWriteParquet;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

class HiveMetaQuerier{
    static SqlSessionFactory sqlSessionFactory;
    static Reader reader;
    static SqlSession session ;
    static tableMapper tableM;
    static sdsMapper sDsM;

    final static String tbdsAppendix = "hdfsCluster";

    static {
        try{
            reader = Resources.getResourceAsReader("mybatis-config.xml");
        }catch (IOException e){
            e.printStackTrace();
        }
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader,"tbds");
        session = sqlSessionFactory.openSession();
        tableM = session.getMapper(tableMapper.class);
        sDsM = session.getMapper(sdsMapper.class);
    }

    public static String getLocation(String tableName){
        Long sID = tableM.selectByTableName(tableName).getSdId();
        String wholeLocation = sDsM.selectByPrimaryKey(sID).getLocation();
        String location = wholeLocation.substring(wholeLocation.indexOf(tbdsAppendix)+tbdsAppendix.length());
        System.out.println(location);

        return location;
    }
}

public class getTable{
    static Configuration conf= new Configuration();
    static final String fuzhouHdfs = "hdfs://10.1.53.205:8020";
    static final String destAppendix = "/tmp/mrzip";
    static final String taskQueue = "root.tbds";

    static {
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("fs.defaultFS", fuzhouHdfs);
    }

    public static void main(String[] args) throws IOException,URISyntaxException{
        //获取当前时间，当需要保存的时候则刷新数据库
        Date startTime = new Date();
        System.out.println(startTime);

        String tableName = args[0];

        //通过在TBDS的HIVE数据库的HIVE metadata获取该表的路径
        String location = HiveMetaQuerier.getLocation(tableName);


        // Find a file in case a directory was passed
        FileSystem fileSys= FileSystem.get(new URI(fuzhouHdfs),conf);
        if (fileSys.exists(new Path(fuzhouHdfs+destAppendix+location))){
            System.out.println("the output already Exists");
            System.exit(2);
        }

        boolean firstRun = false;
        SyncLoggerOrm syncOrm = new SyncLoggerOrm();
        syncLog sLog = syncOrm.getSyncLogByName(tableName);

        //如果首次运行则使用starttime,否则使用数据库中上次任务执行的时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        //modification Time是TBDS侧会去更新的一个时间
        if(sLog==null?true:sLog.getModTime()==null) {
            firstRun = true;
            conf.set("yonghui.startTime",sdf.format(startTime)  );
        }else{
            System.out.println(sLog.getModTime());
            conf.set("yonghui.startTime",sdf.format(sLog.getModTime()));
        }

        //设置HDFS地址和是否首次运行
        conf.set("yonghui.hdfs",fuzhouHdfs);
        conf.set("yonghui.firstTime",firstRun?"true":"false");
        conf.set("mapreduce.job.queuename",taskQueue);
        conf.set("yonghui.name",
                String.format("%s_%s_%s","compress" ,args[0],
                        sdf.format(startTime))
        );

        //获取压缩MR任务的参数
        String[] compressArg = new String[]{fuzhouHdfs, location, destAppendix+location ,"gzip" };

        try {
            int res = ToolRunner.run(conf, new TestReadWriteParquet(), compressArg);
            if (res!=0){
                deleteTempDir(location);
                System.exit(3);
            }
            System.out.println("compress succeed:"+res);
        } catch (Exception e) {
            e.printStackTrace();
            deleteTempDir(location);
            System.out.println("got exception");
            System.exit(3);
        }

        Path fuzhouPath = new Path("hdfs://10.1.53.205:8020" + destAppendix);
        Path chongQinPath = new Path("hdfs://10.216.126.151:8020" + destAppendix);

        String shellString =  "hadoop distcp -Ddfs.replication=3 "+
                String.format("-Dmapreduce.job.queuename=%s -Dmapreduce.job.name=%s_%s_%s " ,taskQueue,"copy",args[0],sdf.format(startTime) ) +
                fuzhouPath.toString() + location + " "
                + chongQinPath.toString() +location;
        System.out.println(shellString);

        try{
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

        //更新完同步数据库
        if(firstRun){
            syncOrm.initSyncLog(tableName,startTime);
        }else {
            sLog.setSyncTime(startTime);
            System.out.println("update records: "+syncOrm.UpdateItem(sLog));
        }
        //如果没有设置keep则删除
        if (!args[1].equals("keep")){
            deleteTempDir(location);
        }
        System.exit(0);
    }

    public static void deleteTempDir(String location) throws IOException{
        //删除tmp目录
        String shellString = "hadoop fs -rmr " + destAppendix+location;
        System.out.println(shellString);
        Process process = Runtime.getRuntime().exec(shellString);
        try{
            int exitValue = process.waitFor();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}



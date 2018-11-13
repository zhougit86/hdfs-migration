package hive;

import hive.TBLS.model.syncLog;
import hive.TBLS.persistence.dao.sdsMapper;
import hive.TBLS.persistence.dao.tableMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import pqtRead.TestReadWriteParquet;

import java.io.IOException;
import java.io.Reader;
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
        System.out.println("sql init ok");
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

    static {
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.address", "10.1.53.205:8032");
        conf.set("fs.defaultFS", "hdfs://10.1.53.205:8020");
    }

    public static void main(String[] args) throws IOException{
        //获取当前时间，当需要保存的时候则刷新数据库
        Date startTime = new Date();
        System.out.println(startTime);

        String tableName = args[0];

        //通过在TBDS的HIVE数据库的HIVE metadata获取该表的路径
        String location = HiveMetaQuerier.getLocation(tableName);


        boolean firstRun = false;
        SyncLoggerOrm syncOrm = new SyncLoggerOrm();
        syncLog sLog = syncOrm.getSyncLogByName(tableName);

        //如果首次运行则使用starttime,否则使用数据库中上次任务执行的时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if(sLog==null) {
            firstRun = true;
            conf.set("yonghui.startTime",sdf.format(startTime)  );
        }else{
            System.out.println(sLog.getSyncTime());
            conf.set("yonghui.startTime",sdf.format(sLog.getSyncTime()));
        }

        //设置HDFS地址和是否首次运行
        conf.set("yonghui.hdfs",fuzhouHdfs);
        conf.set("yonghui.firstTime",firstRun?"true":"false");

        //获取压缩MR任务的参数
        String[] compressArg = new String[]{fuzhouHdfs, location, destAppendix+location ,"gzip" };

        try {
            int res = ToolRunner.run(conf, new TestReadWriteParquet(), compressArg);
            if (res!=0){
                deleteTempDir(location);
                System.exit(1);
            }
            System.out.println("compress succeed:"+res);
        } catch (Exception e) {
            e.printStackTrace();
            deleteTempDir(location);
            System.out.println("got exception");
            System.exit(1);
        }

        Path fuzhouPath = new Path("hdfs://10.1.53.205:8020" + destAppendix);
        Path chongQinPath = new Path("hdfs://10.216.126.151:8020" + destAppendix);

        String shellString =  "hadoop distcp -Ddfs.replication=3 "+ fuzhouPath.toString() + location + " " + chongQinPath.toString() +location;
        System.out.println(shellString);

        try{
            Process process = Runtime.getRuntime().exec(shellString);
            int exitValue = process.waitFor();
            if (exitValue!=0){
                deleteTempDir(location);
                System.exit(1);
            }
        }catch (Exception e){
            e.printStackTrace();
            deleteTempDir(location);
            System.exit(1);
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



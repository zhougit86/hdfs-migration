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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhou1 on 2018/11/14.
 */
class cmdRunner extends Thread{
    SyncLoggerOrm syncOrm = new SyncLoggerOrm();
    syncLog logRecord;
    static CountDownLatch taskCdl;

    public cmdRunner(syncLog input){
        this.logRecord = input;
    }

    public static void setTaskCdl(CountDownLatch cdl){
        taskCdl = cdl;
    }

    public void run(){
        System.out.println(logRecord.getPath() + "-" + logRecord.getSyncTime() + "-" +logRecord.getLength());
        logRecord.setLength(1L);
        syncOrm.UpdateItem(logRecord);

        logRecord.setModTime(logRecord.getSyncTime());
        logRecord.setIssynchronized(true);


//        syncOrm.closeSession();
        taskCdl.countDown();
    }

}

public class filterAction {
    public static void main(String[] args){
        SyncLoggerOrm syncOrm = new SyncLoggerOrm();
        LinkedList<syncLog> sLog = syncOrm.selectAllSyncLog();
        System.out.println(sLog.size());
        Iterator<syncLog> sIter = sLog.iterator();
        ExecutorService es = Executors.newFixedThreadPool(5);

        CountDownLatch cdl = new CountDownLatch(sLog.size());
        cmdRunner.setTaskCdl(cdl);

        while (sIter.hasNext()){
            syncLog logRecord = sIter.next();

            //
            if(logRecord.getLength() ==0L){
                es.submit(new cmdRunner(logRecord));
            }else{
                continue;
            }
//            syncOrm.UpdateItem(logRecord);
        }

        try{
            cdl.await();
            es.shutdownNow();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}

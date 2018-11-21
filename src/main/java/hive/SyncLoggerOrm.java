package hive;

import hive.TBLS.model.syncLog;
import hive.TBLS.persistence.dao.syncLogMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;
import java.util.Date;
import java.util.LinkedList;

public class SyncLoggerOrm {
    SqlSessionFactory sqlSessionFactory;
    Reader reader;
//    SqlSession session;
//    syncLogMapper mapper;
    DirInfoHandler dirInfoHandler;

    public SyncLoggerOrm(){
        try {
            reader = Resources.getResourceAsReader("mybatis-config.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader, "development");

        System.out.println("Init sync_log SqlSession success");
    }



    public LinkedList<syncLog> selectAllSyncLog(){
        SqlSession session = sqlSessionFactory.openSession();
        syncLogMapper mapper = session.getMapper(syncLogMapper.class);
        LinkedList<syncLog> result = mapper.selectAllSyncLog();
        session.close();
        return result;
    }

    public boolean Init(DirInfoHandler dirInfoHandler) {

        this.dirInfoHandler = dirInfoHandler;


        return true;
    }

    public int UpdateItem(syncLog item){
        SqlSession session = sqlSessionFactory.openSession();
        syncLogMapper mapper = session.getMapper(syncLogMapper.class);

        int updateNum = mapper.updateByPrimaryKeySelective(item);
        session.commit();
        session.close();
        return updateNum;
    }

//    public void closeSession(){
//        session.close();
//    }

//    public boolean UpdateSyncInfo(String tableDir) {
//        tableDir = trimEndSlash(tableDir);
//        LinkedList<dirInfo> dirInfos = getDirInfosByTableDir(tableDir);
//        int result = deleteSyncLogsByTableDir(tableDir);
//        System.out.println("deleteSyncLogsByTableDir result = " + result);
//
//        if(!insertDirInfosIntoSyncLog(dirInfos)){
//            System.out.println("insertDirInfosIntoSyncLog() failed!");
//            return false;
//        }
//
//        return true;
//    }

//    public boolean insertDirInfosIntoSyncLog(LinkedList<dirInfo> dirInfos) {
//        for (dirInfo info : dirInfos) {
//            syncLog log = new syncLog();
//            initSyncLogByDirInfo(log, info);
//            int result = mapper.insert(log);
//            if(result != 1){
//                System.out.println("insertDirInfoIntoSyncLog failed! path = " + log.getPath());
//                return false;
//            }
//        }
//
//        session.commit();
//        return true;
//    }

    public boolean initSyncLog(String name,Date startTime){
        SqlSession session = sqlSessionFactory.openSession();
        syncLogMapper mapper = session.getMapper(syncLogMapper.class);

        syncLog log = new syncLog();
        log.setPath(name);
        log.setSyncTime(startTime);
        log.setLength(0L);
        //开始时将isSynchronized至为0
        log.setIssynchronized(false);
        int result = mapper.insert(log);
        if(result != 1){
            System.out.println("insertDirInfoIntoSyncLog failed! path = " + log.getPath());
            return false;
        }

        session.commit();
        session.close();
        return true;
    }

//    private void initSyncLogByDirInfo(syncLog log, dirInfo dirInfo) {
//        log.setPath(dirInfo.getPath());
//        log.setIsdir(dirInfo.getIsDir());
//        log.setIssynchronized(true);
//        log.setModTime(dirInfo.getModTime());
//        log.setSyncTime(new Date());
//    }

    public syncLog getSyncLogByName(String name){
        SqlSession session = sqlSessionFactory.openSession();
        syncLogMapper mapper = session.getMapper(syncLogMapper.class);
        syncLog result = mapper.selectByTableName(name);
        return result;
    }

//    public int deleteSyncLogsByTableDir(String tableDir) {
//        int result = mapper.deleteSyncLogsByTableDir(tableDir);
//        session.commit();
//        return result;
//    }
//
//    public LinkedList<dirInfo> getDirInfosByTableDir(String tableDir) {
//        LinkedList<dirInfo> dirInfos = dirInfoHandler.mapper.selectByTableDir(tableDir);
//        for (dirInfo info : dirInfos) {
//            System.out.println(info.getId() + info.getPath());
//        }
//
//        return dirInfos;
//    }
//
//    private String trimEndSlash(String tableDir) {
//        if(tableDir.endsWith("/")){
//            tableDir = tableDir.substring(0, tableDir.length()-1);
//        }
//        return tableDir;
//    }

    public static void main(String[] args) throws IOException {
        SyncLoggerOrm syncLoggerOrm = new SyncLoggerOrm();
        DirInfoHandler dirInfoHandler = new DirInfoHandler();
        dirInfoHandler.Init();
        syncLoggerOrm.Init(dirInfoHandler);
//        syncLoggerOrm.UpdateSyncInfo("/warehouse/dim/dim_shop");
    }
}



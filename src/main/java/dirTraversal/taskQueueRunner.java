package dirTraversal;

import dirTraversal.dao.DirDao;
import dirTraversal.model.Dir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Time;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

class taskQueueRunner implements Runnable{
    private taskQueue finishedQueue;
    private int counter = 0;
    private static int classId = 1;
    private int RunnerId = classId++;
    private Queue<traversaler> localCache = new LinkedList<traversaler>();
    private traversaler currentHandlingT;
    private static int runningT;

    private final Configuration conf = new Configuration();
    private static String DestHdfs;
    private  FileSystem fs;

    private  SqlSessionFactory sqlSessionFactory;
    private  Reader reader;
    private  SqlSession session ;
    private  DirDao dirMapper;
    private int countForCommit = 0;

    {
        try {
            reader = Resources.getResourceAsReader("mybatis-config.xml");
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);
            session = sqlSessionFactory.openSession();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("sql init ok");

        dirMapper = session.getMapper(DirDao.class);
    }

    public static void setNumT(int t){
        runningT = t;
    }

    public static void setDestHdfs(String dest){
        DestHdfs = dest;
    }

    public taskQueueRunner(taskQueue tq) {
        finishedQueue = tq;
        try{
            this.fs = FileSystem.get(new URI(DestHdfs),conf);
        }catch (URISyntaxException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private void writeSql(Dir ddd){
        try {
            dirMapper.insertDir(ddd);
        }catch (PersistenceException e) {
            System.out.println("duplicate");
            e.printStackTrace();
        }
    }

    private traversaler putWriter(FileStatus f){

        String Path = f.getPath().toString();
        Dir ddd = Dir.newDirFromFileStatus(f);
//        try{
//            while (sQueue.size()>=50){
//                System.out.println("put sql fails");
//                TimeUnit.MILLISECONDS.sleep(500);
//            }
//            sQueue.put(ddd);

            writeSql(ddd);
//        }catch (InterruptedException e){
//            System.out.println("putWriter error");
//            e.printStackTrace();
//        }
        traversaler t = new traversaler(f);
        return t;
    }

    private void putNextTraversal(traversaler t){
        try{
            synchronized (finishedQueue){
                while (finishedQueue.size()>=200*runningT){
//                                System.out.println(finishedQueue.size()+"this fails:" + t.getfStatus().getPath().toString());
                    for (int i = 0;i<150;i++){
                        traversaler tTemp = finishedQueue.poll();
//
                        if (tTemp !=null){
                            localCache.offer(tTemp);
                        }
                    }
                }
                finishedQueue.put(t);
            }
//        tq.put(new traversalTask(t));
        }catch (Exception e){
            System.out.println("1" + this );
            e.printStackTrace();
        }
    }

    @Override
    public String toString(){
        return "thread" + RunnerId;
    }

    public void run(){
        System.out.println("i am :"+ this);
        while (!Thread.interrupted()) {
            if (currentHandlingT == null){
                currentHandlingT = localCache.poll();
                if (currentHandlingT==null){
                    try {
                        currentHandlingT = finishedQueue.take();
                    }catch (InterruptedException e){
                        System.out.println(this);
                    }
                }
            }else{
                try{
                    FileStatus[] listStatus = fs.listStatus(this.currentHandlingT.fStatus.getPath());
                    for (FileStatus f: listStatus){


                        traversaler t= putWriter(f);
                        countForCommit++;
                        if (countForCommit==500){
                            session.commit();
                            System.out.println(this + " commiting");
                            countForCommit=0;
                        }
                        if (!t.fStatus.isDirectory()){
                            continue;
                        }
                        putNextTraversal(t);
                    }
                    currentHandlingT = null;
                }catch (IOException e){
                    System.out.println("2" + this );
                    e.printStackTrace();
                }
            }
        }
        System.out.println(this+ " ended");
        session.commit();
        System.out.println(this+ new Timestamp(Time.now()).toString());
    }
}
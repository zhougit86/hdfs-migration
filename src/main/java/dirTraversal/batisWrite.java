package dirTraversal;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import dirTraversal.model.Dir;
import dirTraversal.dao.DirDao;

import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

/**
 * Created by zhou1 on 2018/10/30.
 */

@Deprecated
public class batisWrite implements Runnable{
    private static SqlSessionFactory sqlSessionFactory;
    private static Reader reader;
    private static sqlQueue sq;
    private static SqlSession session ;
    private static DirDao dirMapper;
    private static endNotifyQueue endQ;

    static {
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

    public static SqlSessionFactory getSqlSessionFactory() {
        return sqlSessionFactory;
    }
    public static void setQueue(sqlQueue sq1,endNotifyQueue endQ1){
        sq = sq1;
        endQ = endQ1;
    }
    public void run() {
        int countForCommit = 0;
        try {
            while (!Thread.interrupted()) {
                Dir t = sq.take();
//                System.out.println("in the Writer:" +t);
                try {
                    dirMapper.insertDir(t);
                    countForCommit++;

                    if(countForCommit==70){
                        System.out.println(countForCommit);
                        countForCommit=0;
                        session.commit();
                    }
                    if (sq.size()==0){
                        System.out.println("finally");
                        session.commit();
                    }
                }catch (Exception e) {
                    System.out.println("failure");
                    e.printStackTrace();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            session.close();
        }
    }

    public static void main(String[] args){
//        System.out.println("hello world");
        //alias demo
//        Configuration con = sqlSessionFactory.getConfiguration();
//        Map<String, Class<?>> typeMap = con.getTypeAliasRegistry().getTypeAliases();
//        for(Map.Entry<String, Class<?>> entry: typeMap.entrySet()) {
//            System.out.println(entry.getKey() + " ================> " + entry.getValue().getName());
//        }


        try {

            Dir u = new Dir();
            u.setDir(false);
            u.setPath("13131");
            u.setMod_Time(new Timestamp(1539687654516L));
            dirMapper.insertDir(u);
            session.commit();

//            User user = userMapper.findUserById(2);
////            User user = (User)session.selectOne("selectUserByID", 2);
////            session.commit();
//            System.out.println(user.getUsername());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.close();
        }

    }
}

package hive;

import hive.TBLS.persistence.dao.dirInfoMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;

/**
 * Created by 杜兴旺 on 2018/11/8.
 */
public class DirInfoHandler {
    SqlSessionFactory sqlSessionFactory;
    Reader reader;
    SqlSession session;
    dirInfoMapper mapper;

    public boolean Init() {
        try {
            reader = Resources.getResourceAsReader("mybatis-config.xml");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader, "development");
        session = sqlSessionFactory.openSession();
        mapper = session.getMapper(dirInfoMapper.class);
        System.out.println("Init dir_info SqlSession success");
        return true;
    }
}

package hive.TBLS.persistence.dao;

import hive.TBLS.model.dirInfo;
import hive.TBLS.model.syncLog;

import java.util.LinkedList;

public interface dirInfoMapper {
    int deleteByPrimaryKey(Long id);

    int insert(dirInfo record);

    int insertSelective(dirInfo record);

    dirInfo selectByPrimaryKey(Long id);

    int updateByPrimaryKeySelective(dirInfo record);

    int updateByPrimaryKey(dirInfo record);

    // tableDir的末尾不要有斜杠（即'/'号）
    LinkedList<dirInfo> selectByTableDir(String tableDir);
}
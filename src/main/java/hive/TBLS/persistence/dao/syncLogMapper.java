package hive.TBLS.persistence.dao;

import hive.TBLS.model.syncLog;

import java.util.LinkedList;

public interface syncLogMapper {
    int deleteByPrimaryKey(Integer syncId);

    int insert(syncLog record);

    int insertSelective(syncLog record);

    syncLog selectByPrimaryKey(Integer syncId);

    syncLog selectByTableName(String pathName);

    int updateByPrimaryKeySelective(syncLog record);

    int updateByPrimaryKey(syncLog record);

    // tableDir的末尾不要有斜杠（即'/'号）
    LinkedList<syncLog> selectByTableDir(String tableDir);

    // tableDir的末尾不要有斜杠（即'/'号）
    int deleteSyncLogsByTableDir(String tableDir);
}
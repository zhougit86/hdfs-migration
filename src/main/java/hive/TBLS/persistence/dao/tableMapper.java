package hive.TBLS.persistence.dao;

import hive.TBLS.model.table;
import hive.TBLS.model.tableWithBLOBs;

public interface tableMapper {
    int deleteByPrimaryKey(Long tblId);

    int insert(tableWithBLOBs record);

    int insertSelective(tableWithBLOBs record);

    tableWithBLOBs selectByPrimaryKey(Long tblId);

    tableWithBLOBs selectByTableName(String tblName);

    int updateByPrimaryKeySelective(tableWithBLOBs record);

    int updateByPrimaryKeyWithBLOBs(tableWithBLOBs record);

    int updateByPrimaryKey(table record);
}
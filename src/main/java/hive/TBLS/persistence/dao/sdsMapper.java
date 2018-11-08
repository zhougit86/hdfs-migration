package hive.TBLS.persistence.dao;

import hive.TBLS.model.sds;

public interface sdsMapper {
    int deleteByPrimaryKey(Long sdId);

    int insert(sds record);

    int insertSelective(sds record);

    sds selectByPrimaryKey(Long sdId);

    int updateByPrimaryKeySelective(sds record);

    int updateByPrimaryKey(sds record);
}
package pqtRead;

import org.apache.parquet.example.data.Group;

/**
 * Created by zhou1 on 2018/11/6.
 */
public class GroupWithFileName  {
    private String fileName;
    public Group group;
    public GroupWithFileName(String fileName1,Group g){
        this.fileName = fileName1;
        this.group = g;
    }
    public Group GetGroup(){
        return this.group;
    }
}
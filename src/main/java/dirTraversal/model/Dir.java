package dirTraversal.model;

import java.sql.Timestamp;
import org.apache.hadoop.fs.FileStatus;

/**
 * Created by zhou1 on 2018/10/30.
 */
public class Dir {
    private int id;
    private String path;
    private boolean isDir;
    private long length;
    private Timestamp mod_Time;
    private String owner;

    @Override
    public String toString(){
        return path;
    }

    public static Dir newDirFromFileStatus(FileStatus fState){
        Dir d = new Dir();
        d.setPath(fState.getPath().toString());
        d.setDir(fState.isDirectory());
        d.setLength(fState.getLen());
        d.setMod_Time(new Timestamp(fState.getModificationTime()));
        d.setOwner(fState.getOwner());

        return d;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isDir() {
        return isDir;
    }

    public void setDir(boolean dir) {
        isDir = dir;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public Timestamp getMod_Time() {
        return mod_Time;
    }

    public void setMod_Time(Timestamp mod_Time) {
        this.mod_Time = mod_Time;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }
}

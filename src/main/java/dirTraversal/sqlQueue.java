package dirTraversal;

import dirTraversal.model.Dir;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zhou1 on 2018/10/30.
 */
class sqlQueue extends LinkedBlockingQueue<Dir> {
    public sqlQueue(int i){
        super(i);
    }
    public sqlQueue(){
        super();
    }
}
package dirTraversal;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zhou1 on 2018/10/30.
 */
class endNotifyQueue extends LinkedBlockingQueue<Object> {
    public endNotifyQueue(int i){
        super(i);
    }
    public endNotifyQueue(){
        super();
    }
}
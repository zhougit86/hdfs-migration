package dirTraversal;

import java.util.concurrent.LinkedBlockingQueue;

class taskQueue extends LinkedBlockingQueue<traversaler> {
    public taskQueue(int i){
        super(i);
    }
    public taskQueue(){
        super();
    }
}
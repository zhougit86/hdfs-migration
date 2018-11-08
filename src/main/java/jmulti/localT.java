package jmulti;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhou1 on 2018/11/4.
 */

public class localT extends Thread {
    private static int globalId;
    private static CountDownLatch cl;
    private static ThreadLocal<Integer> safeInt
        =new ThreadLocal<Integer>(){
            public Integer initialValue(){
                return new Integer(9);
            }
    };

    private int myId = globalId++;

    public static void setCDL(CountDownLatch cl1){
        cl = cl1;
    }

    @Override
    public String toString(){
        StringBuffer sBuffer = new StringBuffer();
        sBuffer.append(safeInt.get());
        sBuffer.append(" __ i am #");
        sBuffer.append(myId);

        return sBuffer.toString();
    }

    @Override
    public void run(){
        int count =0;
        safeInt.set(safeInt.get()+myId);
        while (!interrupted()){
            try{
                TimeUnit.SECONDS.sleep(1);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            if (count++==3){
                synchronized (cl){
                    if(myId<3){
                        System.out.printf("Thread#%d is done\n",myId);
                        cl.countDown();
                    }
                }
                return;
            }
            System.out.println(this);
        }
    }

    public static void main(String[] args){
        CountDownLatch latch = new CountDownLatch(3);
        ExecutorService exec = Executors.newCachedThreadPool();
        localT.setCDL(latch);

        for(int i =0;i<3;i++){
            exec.execute(new localT());
        }

        try {
            latch.await();
            exec.shutdownNow();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}

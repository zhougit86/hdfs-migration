//package etcd;
//
///**
// * Created by zhou1 on 2019/1/30.
// */
//import mousio.etcd4j.EtcdClient;
//import mousio.etcd4j.promises.EtcdResponsePromise;
//import mousio.etcd4j.responses.EtcdKeysResponse;
//
//import java.io.IOException;
//import java.net.URI;
//import java.util.List;
//
///**
// * @Description
// * @auther bozhu
// * @create 11\ 12\ 2018
// */
//public class EtcdClientAddListenerTest {
//
//    public static void main(String[] args) throws Exception{
//        EtcdClient client = null;
//        client = new EtcdClient(URI.create("http://192.168.13.128:2379"));
//
//        for (int i = 1; i < 5000; i++) {
//            EtcdKeysResponse etcdKeysResponse = client.put("/xdriver/test/value", "" + i).send().get();
//            System.out.println(etcdKeysResponse);
//        }
//    }
//
//    private String getConfig(String configFile , EtcdKeysResponse dataTree) {
//        List<EtcdKeysResponse.EtcdNode> nodes = null;
//        if(null != dataTree ) {
//            return dataTree.getNode().getValue();
//        }
//        System.out.println("Etcd configFile"+ configFile+"is not exist,Please Check");
//        return null;
//    }
////    @Test
////    public void testStask() throws Exception {
////
////    }
////    @Test
////    public void testListener() {
////        this.startListenerThread(client , "/xdriver/test/value");
////        Thread.sleep(1000000L);
////    }
//    public void startListenerThread(EtcdClient client , String dir) throws Exception{
//        EtcdKeysResponse etcdKeysResponse = client.get(dir).send().get();
//        System.out.println(etcdKeysResponse.getNode().getValue());
//        new Thread(()->{
//            startListener(client,dir,etcdKeysResponse.getNode().getModifiedIndex() + 1);
//        }).start();
//    }
//    public void startListener(final EtcdClient client , final String dir , long waitIndex)  {
//        EtcdResponsePromise<EtcdKeysResponse> promise = null;
//        try {
//            // 如果监听的waitIndex 不存在与key对应的EventHistroy 中（currentIndex >= waitIndex） ，
//            // 并且key对应的modifyIndex > waitIndex , 则会查找到第一个大于waitIndex 的数据进行展示
//            promise = client.get(dir).waitForChange(waitIndex).consistent().send();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        promise.addListener(promisea -> {
//            try {
//                EtcdKeysResponse etcdKeysResponse = promisea.get();
//                new Thread(() -> {startListener(client , dir , etcdKeysResponse.getNode().getModifiedIndex() + 1);}).start();
//                String config = getConfig(dir, etcdKeysResponse);//加载配置项
//                System.out.println(config);
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.out.println("listen etcd 's config change cause exception:{}"+e.getMessage());
//            }
//        });
//    }
//}
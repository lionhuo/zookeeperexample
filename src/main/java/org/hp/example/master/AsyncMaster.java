/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p>
 * Created on 2019/3/27.
 */
package org.hp.example.master;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class AsyncMaster implements Watcher {
    private ZooKeeper zk;
    private String hostPort;
    String serverId = Integer.toString(new Random().nextInt());
    static boolean isLeader = false;
//    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
//        @Override
//        public void processResult(int i, String s, Object o, String s1) {
//            switch(KeeperException.Code.get(i)) {
//                case CONNECTIONLOSS:
//                    checkMaster();
//                    return;
//                case OK:
//                    isLeader = true;
//                    break;
//                default:
//                    isLeader = false;
//            }
//            System.out.println("I'm " + (isLeader ? "" : "not ") +
//                    "the leader");
//        }
//    };

//    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
//        @Override
//        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
//            switch(KeeperException.Code.get(i)) {
//                case CONNECTIONLOSS:
//                    checkMaster();
//                    return;
//                case NONODE:
//                    runForMaster();
//                    return;
//            }
//        }
//    };

    AsyncMaster(String hostPort){
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    void runForMaster(){
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
            ((int i, String s, Object o, String s1) -> {
                switch(KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        checkMaster();
                        return;
                    case OK:
                        isLeader = true;
                        break;
                    default:
                        isLeader = false;
                }
                System.out.println("I'm " + (isLeader ? "" : "not ") +
                        "the leader");
            }), null);
    }

    void checkMaster(){
        zk.getData("/master", false,
            (int i, String s, Object o, byte[] bytes, Stat stat) ->{
                switch(KeeperException.Code.get(i)) {
                    case CONNECTIONLOSS:
                        checkMaster();
                        return;
                    case NONODE:
                        runForMaster();
                        return;
                }
            }, null);
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        AsyncMaster m = new AsyncMaster("192.168.1.25:2181");
        m.startZK();
        m.runForMaster();
        Thread.sleep(60000);
        if(isLeader){
            System.out.println("I'm a leader");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else {
            System.out.println("Someone else is the leader");
        }
        m.stopZK();
    }
}

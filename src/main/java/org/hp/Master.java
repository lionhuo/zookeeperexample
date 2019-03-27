/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p>
 * Created on 2019/3/24.
 */
package org.hp;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

public class Master implements Watcher {

    private ZooKeeper zk;
    private String hostPort;
    String serverId = Integer.toString(new Random().nextInt());
    static boolean isLeader = false;

    Master(String hostPort){
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }
    
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    boolean checkMaster() throws KeeperException, InterruptedException {
        while(true){
            try{
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equalsIgnoreCase(serverId);
                return true;
            }catch (KeeperException.NoNodeException e){
                return false;
            }

        }
    }

    void runForMaster() throws KeeperException, InterruptedException {
        while(true){
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
            } catch (InterruptedException e) {
            }
            if(checkMaster()){
                break;
            }
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        Master m = new Master("192.168.1.25:2181");
        m.startZK();
        m.runForMaster();
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

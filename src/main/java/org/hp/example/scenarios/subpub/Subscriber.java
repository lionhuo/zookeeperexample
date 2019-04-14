/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p>
 * Created on 2019/4/13.
 */
package org.hp.example.scenarios.subpub;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

public class Subscriber implements Watcher {
    static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ZooKeeper zk;
    private String hostPort;
    private String nodePath;
    private String filePath;
    private static String PARENT_PATH = "/config/lms/agent";
    private Stat stat = new Stat();

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
        if(watchedEvent.getType() == Event.EventType.NodeDataChanged){
            subscribe();
        }
    }

    public Subscriber(String hostPort, String nodePath, String filePath){
        this.hostPort = hostPort;
        this.nodePath = nodePath;
        this.filePath = filePath;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void subscribe(){
        try {
            byte[] data = zk.getData(PARENT_PATH + "/" + nodePath, true, stat);
            System.out.println(new String(data));
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //params
        String hostPort = args[0];
        String nodePath = args[1];
        String filePath = args[2];

        Subscriber subscriber = new Subscriber(hostPort, nodePath, filePath);
        try {
            subscriber.startZK();
            subscriber.subscribe();

            Thread.sleep(Long.MAX_VALUE);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

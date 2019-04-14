/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p>
 * Created on 2019/4/13.
 */
package org.hp.example.scenarios.subpub;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;

public class Publisher implements Watcher {
    static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ZooKeeper zk;
    private String hostPort;
    private static String INITIAL = "initial";
    private static String MODIFY = "modify";
    private static String path = "/";

    public Publisher(String hostPort){
        this.hostPort = hostPort;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("====> watched: " + watchedEvent);
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    private void createPublishNode(){
//        zk.exists("", this)
    }

    private void initial(String pNodePath, String cNodePath, String initialFilePath){
        createParentNodePath(pNodePath);
        createChildNodePath(pNodePath, cNodePath);
        setDataToCNodePath(pNodePath, cNodePath, initialFilePath);
    }

    private void createParentNodePath(String pNodePath){
        Arrays.stream(pNodePath.split("\\/")).forEach(node -> {
            try {
                if(!node.isEmpty()){
                    path += node;
                    zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    path += "/";
                }
            } catch (KeeperException | InterruptedException e) {
                logger.error("===> create parent node path failed. path name:{}", path, e);
            }
        });
    }

    private void createChildNodePath(String pNodePath, String cNodePath){
        Arrays.stream(cNodePath.split(",")).forEach(node -> {
            if(!node.isEmpty()){
                try {
                    String nodePath = pNodePath + "/" + node;
                    zk.create(nodePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException | InterruptedException e) {
                    logger.error("===> create child node path failed. node name:{}", node, e);
                }
            }
        });
    }

    private void setDataToCNodePath(String pNodePath, String cNodePath, String initialFilePath){
        Arrays.stream(cNodePath.split(",")).forEach(node -> {
            if(!node.isEmpty()){
                try {
                    String data = readFromFilePath(node, initialFilePath);
                    String nodePath = pNodePath + "/" + node;
                    zk.setData(nodePath, data.getBytes(), 0);
                } catch (KeeperException | InterruptedException e) {
                    logger.error("===> set node data failed. child node:{}", node, e);
                }
            }
        });
    }

    private String readFromFilePath(String node, String initialFilePath){
        try (FileReader fr = new FileReader(new File(initialFilePath));
             BufferedReader br = new BufferedReader(fr)) {
            String data = "";
            String content = "";
            while ((content = br.readLine()) != null){
                data += content + "\n";
            }
            return data;
        }catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    private void clearNodePath(String pNodePath, String cNodePath){
        final String tmpPNode = pNodePath;
        //clear child node path
        Arrays.stream(cNodePath.split(",")).forEach(node -> {
            try {
                zk.delete(tmpPNode + "/" + node, -1);
            } catch (InterruptedException | KeeperException e) {
                logger.error("===> delete child node path failed. child node:{}", node, e);
            }
        });

        //clear parent node path
        do{
            try {
                zk.delete(pNodePath, -1);
                pNodePath = pNodePath.substring(0, pNodePath.lastIndexOf("/"));
            } catch (KeeperException | InterruptedException e) {
                logger.error("===> delete parent node path failed", e);
                break;
            }

        }while(!pNodePath.isEmpty());
    }

    public static void main(String[] args) { //args0:hostPort, args1:operate type, args2:parent node path args2:child node path, args3:initial file path/modify file path
        //params
        String hostPort = args[0];
        String opType = args[1];
        String pNodePath = args[2];
        String cNodePath = args[3];
        String filePath = args[4];
        logger.info("===> params, hostPort:{}, opType:{}, pNodePath:{}, cNodePath:{}, filePath:{}", hostPort, opType, pNodePath, cNodePath, filePath);
        //create zkClient
        Publisher publisher = new Publisher(hostPort);
        //startZK
        try {
            publisher.startZK();
        } catch (IOException e) {
            logger.error("===> start ZK failed, {}", e.getMessage(), e);
        }
        //execute
        if(opType.equalsIgnoreCase(INITIAL)){
            publisher.clearNodePath(pNodePath, cNodePath);
            publisher.initial(pNodePath, cNodePath, filePath);
        }else if(opType.equalsIgnoreCase(MODIFY)){

        }
    }
}

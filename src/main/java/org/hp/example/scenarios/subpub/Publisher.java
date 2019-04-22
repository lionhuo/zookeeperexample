/**
 * Copyright (c) 2012 Conversant Solutions. All rights reserved.
 * <p>
 * Created on 2019/4/13.
 */
package org.hp.example.scenarios.subpub;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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
    private static String UPDATE = "modify";
    private static String ROOT_PATH = "/config";
    private static String SUBLIST_PATH = "/config/sublist";

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

    /**
     * initial config the node path and the node data
     * @param cNodePath
     * @param initialFilePath
     */
    private void initial(String cNodePath, String initialFilePath){
        //create subscribe root path
        createNodeRecursion(SUBLIST_PATH);
        Arrays.stream(cNodePath.split(",")).forEach(node -> {
            try {
                String subList = new String(zk.getData(SUBLIST_PATH + "/" + node, false, null), "UTF-8");
                logger.info("===> node:{} subscribe list:{}", node, subList);
                createSubListNodePath(subList);
                setSubListNodeData(subList, initialFilePath);
            } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
                logger.error("===> get subscribe list failed. message:{}, node:{}", e.getMessage(), node, e);
            }
        });
    }

    /**
     * create subscribe list node path
     * @param subList
     */
    private void createSubListNodePath(String subList){
        Arrays.stream(subList.split(",")).forEach(nodePath -> {
            createNodeRecursion(ROOT_PATH + nodePath);
        });
    }

    /**
     * create node by recursion
     * @param nodePath
     */
    private void createNodeRecursion(String nodePath){
        String path = "/";
        for(String node : nodePath.split("\\/")){
            try {
                if(!node.isEmpty()){
                    path += node;
                    if(zk.exists(path, false) == null){
                        zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    path += "/";
                }
            } catch (KeeperException | InterruptedException e) {
                logger.error("===> create parent node path failed. path name:{}", path, e);
            }
        }
        logger.info("===> create node recursion success. node path:{}", nodePath);
    }

    private void setSubListNodeData(String subList, String initialFilePath){
        Arrays.stream(subList.split(",")).forEach(nodePath -> {
            String fileName = nodePath.replaceAll("\\/", ".");
            fileName = fileName.substring(fileName.indexOf(".") + 1);
            logger.info("===> subscribe node data file name:{}", fileName);
            String filePath = initialFilePath + "/" + fileName;
            setDataToNodePath(ROOT_PATH + nodePath, filePath);
        });
    }

    /**
     * set the data to the node
     * @param node
     * @param filePath
     */
    private void setDataToNodePath(String node, String filePath){
        logger.info("===> set data file path:{}", filePath);
        try {
            String data = readFromFilePath(filePath);
            zk.setData(node, data.getBytes(), -1);
        } catch (KeeperException | InterruptedException e) {
            logger.error("===> set node data failed. node:{}", node, e);
        }
    }

    /**
     * read the data from the file
     * @param initialFilePath
     * @return
     */
    private String readFromFilePath(String initialFilePath){
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

    /**
     * clear the node by recursion
     * @param pNodePath
     * @param cNodePath
     */
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

    /**
     * update the node data 
     * @param cNodePath
     * @param updateFilePath
     */
    private void update(String cNodePath, String updateFilePath){
        Arrays.stream(cNodePath.split(",")).forEach(node -> {
            if(!node.isEmpty()){
                try {
                    String updateData = readFromFilePath(updateFilePath);
                    String nodeData = new String(zk.getData("/" + node, true, null), "UTF-8");
                    zk.setData("/" + node, (nodeData + updateData).getBytes(), -1);
                } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
                    logger.error("===> get node data failed. node:{}", node);
                }
            }
        });
    }

    public static void main(String[] args) { //args0:hostPort, args1:operate type, args2:child node path, args3:initial file path/update file path
        //params
        String hostPort = args[0];
        String opType = args[1];
        String cNodePath = args[2];
        String filePath = args[3];
        logger.info("===> params, hostPort:{}, opType:{}, cNodePath:{}, filePath:{}", hostPort, opType,cNodePath, filePath);
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
//            publisher.clearNodePath(pNodePath, cNodePath);
            publisher.initial(cNodePath, filePath);
        }else if(opType.equalsIgnoreCase(UPDATE)){
            publisher.update(cNodePath, filePath);
        }
    }
}

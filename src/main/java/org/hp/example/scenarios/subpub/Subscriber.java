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

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class Subscriber implements Watcher {
    static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private ZooKeeper zk;
    private String hostPort;
    private String nodePath;
    private String localFilePath;
    private static String SUBLIST_PATH = "/swiftcoder/config/sublist";
    private static String ROOT_PATH = "/swiftcoder/config";
    private Stat stat = new Stat();
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
        if(watchedEvent.getType() == Event.EventType.NodeDataChanged){
            logger.info("===> node data changed. changed node path:{}", watchedEvent.getPath());
            retrieveNodeData(watchedEvent.getPath());
        }
    }

    public Subscriber(String hostPort, String nodePath, String localFilePath){
        this.hostPort = hostPort;
        this.nodePath = nodePath;
        this.localFilePath = localFilePath;
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public String getSubList(){
        try {
            byte[] data = zk.getData(SUBLIST_PATH + "/" + nodePath, false, null);
            String dataStr = new String(data, "UTF-8");
            System.out.println("===> sub data list:" + dataStr);
            return dataStr;
        } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
            logger.error("===> initial subscribe list failed. message:{}, node:{}", nodePath, e.getMessage(), e);
            return null;
        }
    }

    public void subscribe(String subList){
        Arrays.stream(subList.split(",")).forEach(subNode -> {
            try {
                String nodePath = ROOT_PATH + subNode;
                Stat stat = zk.exists(nodePath, true);
                logger.info("===> subscribe success. node path:{}, node stat:{}", nodePath, stat);
                retrieveNodeData(nodePath);
            } catch (KeeperException | InterruptedException e) {
                logger.error("===> subscribe node filed. message:{}, subNode:{}", e.getMessage(), subNode, e);
            }
        });
    }

    public void retrieveNodeData(String nodePath){
        //nodePath: /swiftcoder/config/192.168.1.25/lms/agent/env.conf  /swiftcoder/config/common/ta_script
        //writeToLocalFile: ${date}/lms.agent/env.conf
        try {
            byte[] data = zk.getData(nodePath, true, stat);
//            String fileName = nodePath.replaceAll("\\/", ".");
//            fileName = fileName.substring(fileName.indexOf(".") + 1);
            String[] pathArray = nodePath.split("\\/");
            String fileName = pathArray[pathArray.length - 1];
            String directoryName = dateFormat.format(new Date()) + "/" + pathArray[pathArray.length - 3] + "." + pathArray[pathArray.length - 2];

            File fileDir = new File(localFilePath + "/" + directoryName);
            fileDir.mkdirs();

            String filePath = localFilePath + "/" + directoryName + "/" + fileName;
            logger.info("===> retrieve node data. file path:{}", filePath);
            writeToLocalFile(new String(data, "UTF-8"), filePath);
        } catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }



    public void writeToLocalFile(String data, String filePath){
        try(FileWriter fw = new FileWriter(new File(filePath));
            BufferedWriter bw = new BufferedWriter(fw)){
            bw.write(data);
        } catch (IOException e) {
            logger.error("===> write to local file failed. message:{}", e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        //params
        String hostPort = args[0];
        String nodePath = args[1];
        String localFilePath = args[2];

        Subscriber subscriber = new Subscriber(hostPort, nodePath, localFilePath);
        try {
            subscriber.startZK();
            String subList = subscriber.getSubList();
            subscriber.subscribe(subList);

            Thread.sleep(Long.MAX_VALUE);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

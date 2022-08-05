package com.example.zookeeper.lock;

import lombok.SneakyThrows;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {

    private final ZooKeeper zk;
    private final CountDownLatch connectLatch = new CountDownLatch(1);
    private final CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath;
    private String currentMode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        // 获取连接
        String connectString = "localhost:2181";
        int sessionTimeout = 2000;
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // connectLatch  如果连接上zk  可以释放
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }

                // waitLatch  需要释放
                if (watchedEvent.getType()== Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }
            }
        });

        // 等待zk正常连接后，往下走程序
        connectLatch.await();

        // 判断根节点/locks是否存在
        Stat stat = zk.exists("/locks", false);

        if (stat == null) {
            // 创建一下根节点
            zk.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 对zk加锁
    @SneakyThrows
    public void zklock() {
        // 创建对应的临时带序号节点
        currentMode = zk.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // wait一小会, 让结果更清晰一些
        Thread.sleep(10);

        // 判断创建的节点是否是最小的序号节点，如果是获取到锁；如果不是，监听他序号前一个节点

        List<String> children = zk.getChildren("/locks", false);

        // 如果children 只有一个值，那就直接获取锁； 如果有多个节点，需要判断，谁最小
        if (children.size() == 1) {
            return;
        } else {
            Collections.sort(children);

            // 获取节点名称 seq-00000000
            String thisNode = currentMode.substring("/locks/".length());
            // 通过seq-00000000获取该节点在children集合的位置
            int index = children.indexOf(thisNode);

            // 判断
            if (index == -1) {
                System.out.println("数据异常");
            } else if (index == 0) {
                // 就一个节点，可以获取锁了
                return;
            } else {
                // 需要监听  他前一个节点变化
                waitPath = "/locks/" + children.get(index - 1);
                zk.getData(waitPath,true,new Stat());

                // 等待监听
                waitLatch.await();

                return;
            }
        }

    }

    // 解锁
    @SneakyThrows
    public void unZkLock() {
        // 删除节点
        zk.delete(this.currentMode,-1);

    }

}
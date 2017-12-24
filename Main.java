
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import javax.swing.*;

public class Main {
    public static void main(String[] args) throws Exception{
        GetData.run();
    }
}

class Create_znode implements Watcher
{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void run() throws Exception
    {
        ZooKeeper zookeeper = new ZooKeeper ("111.230.37.64:2181", 5000, new Create_znode());
        connectedSemaphore.await();
        String path1 = zookeeper.create("/zk-test-ephemeral-", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Success create znode:" + path1);
        String path2 = zookeeper.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Success create znode:" + path2);
    }

    public void process(WatchedEvent event)
    {
        System.out.println("Receive watched event: " + event) ;
        if (KeeperState.SyncConnected == event.getState())
        {
            connectedSemaphore.countDown();
        }
    }
}

class Connect implements Watcher
{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void run() throws Exception
    {
        ZooKeeper zookeeper = new ZooKeeper ("111.230.37.64:2181", 5000, new Connect());
        System.out.println(zookeeper.getState());
        connectedSemaphore.await();
        long sessionId = zookeeper.getSessionId();
        byte[] sessionPasswd = zookeeper.getSessionPasswd();

        zookeeper = new ZooKeeper("111.230.37.64:2181", 5000, new Connect(), sessionId, sessionPasswd);
        Thread.sleep(Integer.MAX_VALUE);

    }

    public void process(WatchedEvent event)
    {
        System.out.println("Receive watched event: " + event) ;
        if (KeeperState.SyncConnected == event.getState())
        {
            connectedSemaphore.countDown();
        }
    }
}

class GetChildren implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;

    public static void run() throws Exception {

        String path = "/zk-book";
        zk = new ZooKeeper("111.230.37.64:2181", 5000, new GetChildren());
        connectedSemaphore.await();
        zk.create(path, "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(path+"/c1", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        List<String> childreList = zk.getChildren(path, true);
        System.out.println(childreList);

        zk.create(path+"/c2", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
            if (EventType.None == event.getType() && null == event.getPath()) {
                connectedSemaphore.countDown();
            } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                try {
                    System.out.println(event);
                    System.out.println("Reget Children: " + zk.getChildren(event.getPath(),true));
                } catch (Exception e) {}
            }
        }
    }
}

class GetData implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk = null;

    public static void run() throws Exception {
        String path = "/zk-book";
        zk = new ZooKeeper("111.230.37.64:2181", 5000, new GetData());
        connectedSemaphore.await();
        zk.create(path, "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Stat stat = new Stat();
        byte[] data = zk.getData(path, new GetData(), stat);
        System.out.println(new String(data));
        System.out.println(stat);
        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
            if (EventType.None == event.getType() && null == event.getPath()) {
                connectedSemaphore.countDown();
            } else if (event.getType() == EventType.NodeDataChanged) {
                try {
                    System.out.println(event);
                    Stat stat = new Stat();
                    byte[] newData = zk.getData(event.getPath(), true, stat);
                    System.out.println("Reget Data: " + new String(newData));
                    System.out.println("Reget Stat: " + stat);
                } catch (Exception e) {}
            }
        }
    }
}

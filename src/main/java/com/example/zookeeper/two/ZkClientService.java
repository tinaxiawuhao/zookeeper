package com.example.zookeeper.two;

import com.example.zookeeper.two.utils.PropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.*;

import static com.example.zookeeper.two.utils.SocketCheckUtil.checkPort;


/**
 * Zookeeper客户端
 */
@Component
@Scope//singleton 单实例
@Slf4j
public class ZkClientService  {
	//zk连接节点编号
	int zkServerNodeNum = 0;
	//命名空间
	private String zkNamespace = "com.start.default";
	//连接超时间
	private int zkConnectTimeout = 2000;
	//集群ip端口列表(zk节点挂了后备用)
	private final List<String> zkServerList = new ArrayList<>();//connectString
	//开关: 当zk节点丢失自动换节点
	private boolean zkAutoChangeServerNode = false;
	//zk编码
	private String zkCharset = "utf-8";
	//连接成功标识
	private boolean initFlag = false;
	//zk客户端
	private CuratorFramework client;
	//已监听节点记录
	private Map<String, Map> zkNodePathCachedMap = new HashMap<>();

	//初始化
	private ZkClientService() {
		if (loadProperties()) {
			boolean connect = false;
			while (!connect) {
				for (int i = 0; i < this.zkServerList.size(); i++) {
					this.zkServerNodeNum = i;
					connect = connect(this.zkServerNodeNum);
				}
				if (!connect) {
					log.error("所有ZK节点连接失败将在15秒后重新尝试连接.");
					try {
						Thread.sleep(15000);
					} catch (InterruptedException e) {
						log.error("线程暂停15秒错误!",e);
					}
				}
			}
		} else {
			log.error("ZK客户端启动失败,无法载入配置文件");
		}
	}

	//读取配置信息
	private boolean loadProperties() {
		//载入配置信息
		//读取集群列表
		String  zookeeperServerListProp = PropertiesUtil.getZkClient().getProperty("zkServerList");
		if (log.isDebugEnabled()) {
			log.debug("读取ZK节点配置: " + zookeeperServerListProp);
		}
		if (StringUtils.isNotBlank(zookeeperServerListProp)) {
			String[] split = zookeeperServerListProp.split(",");
			for (String connectionString : split) {
				if (connectionString.contains(":")) {
					addZkServerList(connectionString);
				} else {
					log.error("ZK配置文件错误:" + "不正确的节点格式  " + connectionString);
				}
			}
			if (zkServerList.size() == 0) {
				log.error("ZK节点配置失败:" + "失效或节点信息不可用");
				return false;
			}
		} else {
			log.error("Zookeeper客户端无法启动!无法获取节点信息,请检查zkClient.properties");
			return false;
		}

		//读取Charset配置
		String charsetProp = PropertiesUtil.getZkClient().getProperty("zkCharset");
		if (StringUtils.isNotBlank(charsetProp) && Charset.isSupported(charsetProp)) {
			setZkCharset(charsetProp.toLowerCase());
		} else {
			log.warn("Zookeeper client zkCharset is not support. zkCharset=" + charsetProp + " ,used default zkCharset : " + this.zkCharset);
		}

		//读取namespace
		String namespaceProp = PropertiesUtil.getZkClient().getProperty("zkNamespace");
		if (StringUtils.isBlank(namespaceProp)) {
			log.warn("Zookeeper未配置正确命名空间,已使用 " + this.zkNamespace + " 作为命名空间");
		} else {
			setZkNamespace(namespaceProp);
		}

		//读取nodeNumber zkServerNodeNum
		try {
			this.zkServerNodeNum = Integer.parseInt(PropertiesUtil.getZkClient().getProperty("zkServerNodeNum"));
			if (this.zkServerNodeNum >= zookeeperServerListProp.length()) {
				this.zkServerNodeNum = zookeeperServerListProp.length() - 1;
				log.warn("警告: 不存在的节点 " + PropertiesUtil.getZkClient().getProperty("zkServerNodeNum") + " ,已使用节点 " + this.zkServerNodeNum + " 进行连接");
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.warn("Zookeeper未配置正确节点编号,已使用 " + this.zkServerNodeNum + " 作为初始节点");
		}
		//读取连接超时时间
		try {
			this.zkConnectTimeout = Integer.parseInt(PropertiesUtil.getZkClient().getProperty("zkConnectTimeout"));
		} catch (Exception e) {
			e.printStackTrace();
			log.warn("Zookeeper未配置正确超时时间,已使用 " + this.zkConnectTimeout + " 作为默认超时时间");
		}

		//读取自动更换节点
		try {
			this.zkAutoChangeServerNode = "true".equals(PropertiesUtil.getZkClient().getProperty("zkAutoChangeServerNode"));
		} catch (Exception e) {
			e.printStackTrace();
			log.warn("Zookeeper未配置正确节点更换策略, 已关闭自动更换节点");
		}
		return true;
	}

	public void addZkServerList(String connectionString) {
		if (StringUtils.isNotBlank(connectionString) && !this.zkServerList.contains(connectionString) /*&& checkConnectionString(connectionString)*/) {
			this.zkServerList.add(connectionString);
			log.info("添加ZK服务节点配置: " + connectionString);
		} else {
			log.info("无法添加ZK服务节点配置,失效或已存在的节点信息: " + connectionString);
		}
	}

	public boolean connect(int zkServerNodeNum) {
		if (zkServerNodeNum >= zkServerList.size()) {
			zkServerNodeNum = zkServerList.size() - 1;
		}
		if (connect(zkServerList.get(zkServerNodeNum))) {
			this.zkServerNodeNum = zkServerNodeNum;
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 连接zk服务器节点
	 *
	 * @param connectionString 连接信息(ip:port)
	 * @return boolean
	 */
	private boolean connect(String connectionString) {
		log.info("尝试连接到zk节点:   " + connectionString + " 命名空间 : " + zkNamespace);

		try {
			if (StringUtils.isBlank(connectionString)) {
				return false;
			}
			if (StringUtils.isBlank(zkNamespace)) {
				log.error("无法获取命名空间,无法启动zk客户端");
				return false;
			}
			if (zkServerList.size() == 0) {
				log.error("未载入ZK节点配置信息,请检查配置文件.ZK客户端启动失败!");
				return false;
			}
			if (null != client && initFlag && connectionString.equals(client.getZookeeperClient().getCurrentConnectionString())) {
				log.warn("连接失败,已经在当前节点: " + connectionString);
				return false;
			}
			if (initFlag) {
				//已初启动,是否允许更换节点
				if (!zkAutoChangeServerNode) {
					log.debug("未开启zk自动切换节点功能");
					return false;
				}
			}
			if (checkConnectionString(connectionString) && zkServerList.contains(connectionString)) {
				doConnect(connectionString);
			}
			//切换所有服务节点(当ZK客户端未连接到ZK服务器时,启动自动更换节点机制,若尝试所有zk节点都失败时,连接ZK服务节点失败)
			for (String zkConnectionString : zkServerList) {
				if (initFlag) {
					break;
				}
				if (!connectionString.equals(zkConnectionString) && checkConnectionString(zkConnectionString)) {
					doConnect(zkConnectionString);
					if (initFlag) {
						break;
					}
				}
			}

			if (!initFlag) {
				log.error("所有ZK节点不可用,ZK客户端启动失败!");
				if (null != client) {
					client.close();
					if (null != client) {
						client.delete();
					}
				}
			}
			return initFlag;
		} catch (Exception e) {
			log.error("连接ZK失败！" + e.getMessage());
			e.printStackTrace();
			return initFlag;
		}
	}

	private void doConnect(String connectionString) {
		try {
			//断开连接
			disConnect();
			initFlag = false;

			//重新建立连接
			this.client = CuratorFrameworkFactory.builder()
					.connectString(connectionString)
					.namespace(zkNamespace)
					.retryPolicy(new ExponentialBackoffRetry(1000, 3))
					.connectionTimeoutMs(1000)
					.sessionTimeoutMs(1000)
					// etc. etc.
					.build();

			//start
			this.client.start();
			if (log.isDebugEnabled()) {
				log.debug("client__  " + client);
			}
			boolean connectState = false;
			int connectTime = 0;
			while (!connectState) {
				//ZooKeeper.States connected = ZooKeeper.States.CONNECTED;
				if (client != null) {
					ZooKeeper.States state = null;
					try {
						state = client.getZookeeperClient().getZooKeeper().getState();
					} catch (Exception e) {
						//e.printStackTrace();
						if (log.isDebugEnabled()) {
							log.debug("获取状态失败: " + e.getMessage());
						}
					}
					connectState = ZooKeeper.States.CONNECTED.equals(state);
				}
				Thread.sleep(10);
				connectTime += 10;
				if (connectTime > zkConnectTimeout) {
					log.error("连接ZK失败！连接超时：" + zkConnectTimeout + "ms");
					disConnect();
					initFlag = false;
					return;
				}
			}
			initFlag = true;
			log.info("已连接到ZK!" + connectionString + " 用时:" + connectTime);
		} catch (Exception e) {
			log.error("连接ZK异常!",e);
		}
	}

	private boolean checkConnectionString(String connectionString) {
		try {
			if (StringUtils.isBlank(connectionString)
					|| connectionString.split(":").length != 2
					|| StringUtils.isBlank(connectionString.split(":")[0])      //ip不为空
					|| new Integer(connectionString.split(":")[1]) < 1000         //zk端口最小1000
					) {
				log.info("无效的连接节点  : " + connectionString);
				return false;
			}
			String ip;
			Integer port;
			ip = connectionString.split(":")[0];
			port = new Integer(connectionString.split(":")[1]);
			//--有些机器可能拒绝ping,更换为采用检测zk端口是否打开的方式判断新zk服务器是否连通
			if (!checkPort(ip, port)) {
				log.error("请求的节点连接端口未开放 IP:" + ip + " PORT:" + port);
				return false;
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			log.info("无效的连接节点 : " + connectionString);
			return false;
		}
	}

	private void disConnect() {
		clearListener();
		if (client != null) {
			client.close();
		}
		client = null;
	}

	private void clearListener() {
		try {
			List<String> listenedList = getListenedList();
			for (String path : listenedList) {
				removeListener(path);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	//-------------------CRUD----------------------
	// 创建一个节点
	public boolean createPath(String path) {
		try {
			boolean pathExist = checkExist(path);
			path = startWith(path);
			if (pathExist) {//节点已存在
				return false;
			}
			String forPath = client.create().forPath(path, new byte[0]);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 删除节点，及子节点
	 *
	 * @param path node
	 * @return boolean
	 */
	// 强制删除一个节点
	public boolean deletePathIncludeChildren(String path) {
		path = startWith(path);
		try {
			if (!checkExist(path)) {//节点不存在
				log.error("删除的zk节点不存在");
				return false;
			}
			//client.delete().inBackground().forPath(path);
			client.delete().forPath(path);
			return true;
		} catch (Exception e) {
			//尝试删除子节点
			if (!"".equals(path) && !"/".equals(path) && null != path) {
				List<String> pathList = getPathList(path);
				boolean deleteChildren = false;
				String fullPath;
				for (String childrenPath : pathList) {
					//递归删除子节点
					fullPath = path + "/" + childrenPath;
					deleteChildren = deletePathIncludeChildren(fullPath);
				}
				//删除父节点自身
				deletePathIncludeChildren(path);
				return deleteChildren;
			} else {
				return false;
			}
		}
	}


	/**
	 * 存放数据，若节点不存在尝试创建节点，创建失败，返回false
	 *
	 * @param path node
	 * @param data content
	 * @return boolean
	 */
	// 存数据（自动创建节点）
	public boolean setPathDataIfNoNode(String path, String data) {
		try {
			//Stat stat1 = client.setData().inBackground().forPath(path, data.getBytes());

			path = startWith(path);
			boolean pathExist = checkExist(path);
			if (pathExist) {
				Stat stat1 = client.setData().forPath(path, data.getBytes(zkCharset));//默认打开支持UTF-8格式中文数据
				return stat1 != null;
			} else {
				boolean createPath = createPath(path);
				if (createPath) {
					Stat stat1 = client.setData().forPath(path, data.getBytes(zkCharset));
					return stat1 != null;
				} else {
					return false;
				}
			}
		} catch (Exception e) {
			return false;
		}
	}

	//----------------CRUD  FOR  GATEWAY MESSAGE----------------
	//配置同步ADMIN端调用
	// 编辑或创建
	public boolean addOrUpdate(String id, /*ZkConstants*/String type) {
		boolean result = false;
		result = setPathDataIfNoNode(type + "/" + id, UUID.randomUUID().toString());
		if(!result)log.error("添加或修改zk节点错误!");
		return result;
	}

	// 批量添加
	public void batchAdd(List<String> ids, String type) {
		for (String id : ids) {
			this.addOrUpdate(id, type);
		}
	}

	// 删除
	public boolean remove(String id, String type) {
		boolean result = false;
		result = deletePathIncludeChildren(type + "/" + id);
		if(!result)log.error("删除zk节点错误!");
		return result;
	}
	//批量删除
	public void batchDel(List<String> ids,String type) {
		boolean success = true;
		for(String id:ids){
			success=deletePathIncludeChildren(type + "/" + id);
		}
		if (!success){
			log.error("删除zk节点错误!");
		}
	}

	//--------------------FUNCTION-----------------
	// 检查节点是否存在
	public boolean checkExist(String path) {
		//修复节点不以 / 开头
		path = startWith(path);
		Stat stat;
		try {
			stat = client.checkExists().forPath(path);
			return stat != null;
		} catch (Exception e) {
			return false;
		}
	}

	//节点必须以/开头
	private String startWith(String path) {
		if (!path.startsWith("/")) {
			path = "/" + path;
		}
		return path;
	}

	//获取节点列表
	public List<String> getPathList(String path) {
		List<String> childrenPath = new ArrayList<>();
		try {
			childrenPath = client.getChildren().forPath(path);
			return childrenPath;
		} catch (Exception e) {
			return childrenPath;
		}
	}

	//获取监听列表
	public List<String> getListenedList() {
		List<String> listenedPathList = new ArrayList<>();
		for (String listenedPath : zkNodePathCachedMap.keySet()) {
			listenedPathList.add(listenedPath);
		}
		return listenedPathList;
	}

	//删除监听
	public void removeListener(String path) {
		try {
			Map stringPathChildrenCacheMap = zkNodePathCachedMap.get(path);
			PathChildrenCache cache = null;
			if (stringPathChildrenCacheMap != null) {
				cache = (PathChildrenCache) stringPathChildrenCacheMap.get("cache");
			}
			if (cache != null) {
				//cache.close();
				cache.clear();
				zkNodePathCachedMap.remove(path);
				stringPathChildrenCacheMap.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setZkCharset(String zkCharset) {
		this.zkCharset = zkCharset;
	}

	public void setZkNamespace(String zkNamespace) {
		this.zkNamespace = zkNamespace;
	}
}
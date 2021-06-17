package com.example.zookeeper.two.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 预检查工具类
 */
public class SocketCheckUtil {


	/**
	 * 开放true//未开发false
	 *
	 * @param ip
	 * @param port
	 * @return
	 */
	//检测指定主机端口是否开放
	public static boolean checkPort(String ip, int port) {
		return checkPort(ip, port, 500);
	}

	/**
	 * 开放true//未开放false
	 *
	 * @param ip
	 * @param port
	 * @param timeout
	 * @return
	 */
	//检测指定主机端口是否开放
	public static boolean checkPort(String ip, int port, int timeout) {
		Socket client = null;
		try {
			client = new Socket();
			InetSocketAddress address = new InetSocketAddress(ip, port);
			client.connect(address, timeout);
			//logger.debug("检测端口 "+ip + ":" + port + "端口已开放");
			client.close();
			return true;
		} catch (Exception e) {
			//logger.debug("检测端口 "+ip + ":" + port + "端口未开放");
			//e.printStackTrace();
			return false;
		} finally {
			if (null != client) {
				try {
					client.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 开放true//未开放false
	 *
	 * @param host
	 * @return
	 */
	//检测指定主机端口是否开放
	public static boolean checkPort(String host) {
		try {
			return checkPort(host.split(":")[0], Integer.parseInt(host.split(":")[1]));
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 开放true//未开放false
	 *
	 * @param timeout
	 * @param host
	 * @return
	 */
	//检测指定主机端口是否开放
	public static boolean checkPort(int timeout, String host) {
		try {
			return checkPort(host.split(":")[0], Integer.parseInt(host.split(":")[1]), timeout);
		} catch (Exception e) {
			return false;
		}
	}
}

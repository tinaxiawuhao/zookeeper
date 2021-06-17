package com.example.zookeeper.two.utils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
@Data
public class PropertiesUtil {

	private static final Properties APPLICATION = new Properties();
	private static final Properties ZK_CLIENT = new Properties();

	static {
		loadProperties(APPLICATION, "/src/main/resources/config/","application.properties");
		loadProperties(ZK_CLIENT, "/src/min/resources/config/","zkClient.properties");
	}

	public static Properties getApplication() {
		return APPLICATION;
	}

	public static Properties getZkClient() {
		return ZK_CLIENT;
	}

	private static void loadProperties(final Properties prop,final String path, final String fileName) {
		InputStream resourceAsStream = null;
		String fileProp = System.getProperty("user.dir").replace("\\", "/") +path + fileName;
		try {
			try {
				resourceAsStream = new FileInputStream(fileProp);
			}catch (FileNotFoundException e){
				resourceAsStream=null;
			}
			try {
				if (null == resourceAsStream) {
					resourceAsStream = new FileInputStream(fileProp.replace("config/", ""));
				}
			}catch (FileNotFoundException e){
				resourceAsStream=null;
			}

			if (null == resourceAsStream) {
				//Class.getResourceAsStream(String path) ： path 不以’/'开头时默认是从此类所在的包下取资源，以’/'开头则是从ClassPath根下获取。其只是通过path构造一个绝对路径，最终还是由ClassLoader获取资源。
				resourceAsStream = Class.class.getResourceAsStream("/config" + fileName);
			}
			if (null == resourceAsStream) {
				//Class.getClassLoader.getResourceAsStream(String path) ：默认则是从ClassPath根下获取，path不能以’/'开头，最终是由ClassLoader获取资源。
				resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
			}
			if (resourceAsStream != null) {
				prop.load(resourceAsStream);
			} else {
				throw new FileNotFoundException("\n无法加载配置文件: " + fileProp);
			}
		} catch (IOException e) {
			log.error("读取配置文件报错，请检查文件是否在路径" + System.getProperty("user.dir") + "下，获取文件内容是否正确！！", e);
		} finally {
			if (resourceAsStream != null) {
				try {
					resourceAsStream.close();
				} catch (IOException e) {
					log.warn("close " + fileName);
				}
			}
		}
	}

}


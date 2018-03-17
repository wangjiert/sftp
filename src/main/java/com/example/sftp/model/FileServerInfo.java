package com.example.sftp.model;

import java.util.Properties;

public class FileServerInfo {

	private String host;
	private int port;
	private String account;
	private String password;
	private String filePath;
	private String localPath;
	private String fileName;
	private String domain;
	private String encode;
	private int max;
	private int timeout;
	private String privateKey;
	private String passphrase;
	private int day;
	
	public FileServerInfo(String host, int port, String username, String password, String localPath, String remoteDir, int max, int day) {
        this.host = host;
        this.port = port;
        this.account = username;
        this.password = password;
        this.localPath = localPath;
        this.filePath = remoteDir;
        this.max = max;
        this.timeout = 60000;
        this.day = day;
    }
	
	public FileServerInfo(Properties prop) {
		this.host = prop.getProperty("remote.host");
		this.port = Integer.parseInt(prop.getProperty("remote.port", "22"));
		this.account = prop.getProperty("remote.username");
		this.password = prop.getProperty("remote.passwd");
		this.filePath = prop.getProperty("remote.dir").replaceAll("\\\\", "/");
		this.localPath = prop.getProperty("local.dir");
		this.max = Integer.parseInt(prop.getProperty("max.thread", "10"));
		this.privateKey = prop.getProperty("key.path");
		this.passphrase = prop.getProperty("key.pwd");
		this.timeout = Integer.parseInt(prop.getProperty("remote.timeout", "60000"));
		this.day = Integer.parseInt(prop.getProperty("day", "1"));
	}

	public int getDay() {
		return this.day;
	}
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public String getLocalPath() {
		return localPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	public String getEncode() {
		return encode;
	}

	public void setEncode(String encode) {
		this.encode = encode;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public String getPrivateKey() {
		return privateKey;
	}

	public void setPrivateKey(String privateKey) {
		this.privateKey = privateKey;
	}

	public String getPassphrase() {
		return passphrase;
	}

	public void setPassphrase(String passphrase) {
		this.passphrase = passphrase;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

}

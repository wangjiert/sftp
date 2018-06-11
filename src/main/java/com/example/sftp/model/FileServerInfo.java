package com.example.sftp.model;

import java.io.File;
import java.nio.file.Paths;
import java.util.Properties;

public class FileServerInfo {
    private String dumpDir; //导入文件存放目录
    private String host;//sftp服务ip
    private int port;//sftp服务端口
    private String account;//sftp用户名
    private String password;//sftp密码
    private String filePath;//sftp服务端下载路径
    private String localPath;//本地下载路径
    private String fileName;
    private String domain;
    private String encode;
    private int max;//最大线程数
    private int timeout;//sftp超时时间
    private String privateKey;//ssh key文件路径,实现免密登录
    private String passphrase;//ssh key加密文件
    private int day;//多少天之前的文件直接删除
    private boolean today;//只下载今天的文件
    private boolean yestoday;//只下载昨天的文件
    private boolean history;//只下载前天以及之前的文件
    private String startIp;//下载文件最小ip
    private String endIp;//下载文件最大ip

    public FileServerInfo(String host, int port, String username, String password, String localPath, String remoteDir, int max, int day) {
        this.host = host;
        this.port = port;
        this.account = username;
        this.password = password;
        this.localPath = localPath;
        this.filePath = remoteDir;
        this.max = max;
        this.timeout = 604800000;
        this.day = day;
    }

    public FileServerInfo(Properties prop) {
        this.dumpDir = Paths.get(prop.getProperty("datafile.data_file_path"), "sqldump").toString();
        this.host = prop.getProperty("remote.host");
        this.port = Integer.parseInt(prop.getProperty("remote.port", "22"));
        this.account = prop.getProperty("remote.username");
        this.password = prop.getProperty("remote.password");
        this.localPath = prop.getProperty("local.dir");
        this.filePath = prop.getProperty("remote.dir").replaceAll("\\\\", "/");
        this.max = Integer.parseInt(prop.getProperty("max.thread", "10"));
        this.max = 1;
        this.privateKey = prop.getProperty("key.path");
        this.passphrase = prop.getProperty("key.pwd");
        this.timeout = Integer.parseInt(prop.getProperty("remote.timeout", "604800000"));
        this.day = Integer.parseInt(prop.getProperty("day", "1"));
        this.today = Boolean.valueOf(prop.getProperty("today", "false"));
        this.yestoday = Boolean.valueOf(prop.getProperty("yestoday", "false"));
        this.history = Boolean.valueOf(prop.getProperty("history", "false"));
        String[] iprange = prop.getProperty("iprange", "").split("-");
        if (iprange.length == 2) {
            this.startIp = iprange[0];
            this.endIp = iprange[1];
        }
    }

    public String getDumpDir() {
        return dumpDir;
    }

    public void setDumpDir(String dumpDir) {
        this.dumpDir = dumpDir;
    }

    public boolean isToday() {
        return today;
    }

    public boolean isYestoday() {
        return yestoday;
    }

    public boolean isHistory() {
        return history;
    }

    public String getStartIp() {
        return startIp;
    }

    public String getEndIp() {
        return endIp;
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

package com.example.sftp.demo;

import com.example.sftp.model.FileServerInfo;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SftpDownload {
    private static Logger logger;
    protected static ChannelSftp globalTransSftp;
    protected static SftpProgressMonitorImpl globalTransListen;

    private static String HOME_PATH;
    public static String PREFIX = "";//sftp下载目录去除最后一级剩余
    protected static String LOCAL = ""; //本地目录

    private static final String CONF_PATH = "conf/conf.properties";
    private static final String dLogName = "download.log";
    private static final String uLogName = "finish.log";

    private static final String[] SUFFIX = {".frm", ".MYD", ".MYI"};

    private static Pattern pattern = Pattern.compile("\\D*(\\d{8})\\D*");
    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    private static String todayStr;
    private static String yesterday;
    private static Date deadline;//小于该日期的文件就删除

    static FileServerInfo fileServerInfo;

    public static void main(String[] args) {
        System.out.println("=============================================================================");
        System.out.printf("=========begin new transmission======time:%s====\n", new Date().toString());
        System.out.println("=============================================================================");
        try {
            //上传
            if (args[1].equals("u")) {
                SftpUpload.upload(args);
                return;
            }
            //有最后的/
            HOME_PATH = System.getProperty("SFTP_HOME");
            logger = LogManager.getLogger(SftpDownload.class);
            Path conf = Paths.get(HOME_PATH, "../conf/my.conf");
            if (Files.exists(conf)) {
                //sftp配置
                Properties prop = ParseConfig.parse(conf, "mydump", null);
                if (prop == null) {
                    return;
                }
                //下载目录配置
                Properties prop1 = ParseConfig.parse(conf, "datafile", "data_file_path");
                if (prop1 == null) {
                    return;
                }
                prop.setProperty("datafile.data_file_path", prop1.getProperty("datafile.data_file_path"));

                fileServerInfo = new FileServerInfo(prop);

                download(fileServerInfo.getHost(), fileServerInfo.getPort(), fileServerInfo.getAccount(),
                        fileServerInfo.getPassword(), fileServerInfo.getLocalPath(), args[0], fileServerInfo.getMax());

            } else {
                //很久没更新了
                initFileServerInfo(args);
                if (fileServerInfo == null) {
                    return;
                }
                download(fileServerInfo.getHost(), fileServerInfo.getPort(), fileServerInfo.getAccount(),
                        fileServerInfo.getPassword(), fileServerInfo.getLocalPath(), args[0], fileServerInfo.getMax());
            }
        } finally {
            DBUtil.close();
            System.out.println("=============================================================================");
            System.out.printf("=========end new transmission=======time:%s=======\n", new Date().toString());
            System.out.println("=============================================================================");
        }
    }

    //检查配置
    private static boolean checkConfig() {
        int num = 0;
        if (fileServerInfo.isHistory()) {
            num++;
        }
        if (fileServerInfo.isYestoday()) {
            num++;
        }
        if (fileServerInfo.isToday()) {
            num++;
        }
        if (num > 1) {
            System.out.printf("time:%s, thread :%s, today history and yestoday can't be true at the same time\n",
                    new Date().toString(), Thread.currentThread().getName());
            return false;
        }
        if (fileServerInfo.getStartIp().equals("") || fileServerInfo.getEndIp().equals("")) {
            System.out.printf("time:%s, thread :%s, iprange format is wrong\n",
                    new Date().toString(), Thread.currentThread().getName());
            return false;
        }

        if (fileServerInfo.getFilePath() == null || fileServerInfo.getFilePath().equals("")) {
            System.out.printf("time:%s, thread :%s, remote filepath can't be null\n", new Date().toString(),
                    Thread.currentThread().getName());
        }

        boolean reachable = checkConnect();
        if (!reachable) {
            return false;
        }
        return true;
    }

    private static void initFileServerInfo(String args[]) {

        Properties prop = new Properties();
        try (FileInputStream fis = new FileInputStream(HOME_PATH + CONF_PATH);) {
            prop.load(fis);
            if (args.length > 0) {
                prop.setProperty("remote.dir", args[0]);
            }
            fileServerInfo = new FileServerInfo(prop);
        } catch (FileNotFoundException e) {
            logger.error("", e);
            System.out.printf("time:%s, thread :%s, please be sure config file's path is correct\n",
                    new Date().toString(), Thread.currentThread().getName());
            return;
        } catch (IOException e) {
            logger.error("", e);
            System.out.printf("time:%s, thread :%s, read file:%s failed, please retry\n", new Date().toString(),
                    Thread.currentThread().getName(), HOME_PATH + "conf/conf.properties");
            return;
        }
    }

    private static Path mkDirs(Path path) {
        Set<PosixFilePermission> permissions = new HashSet<>();
        permissions.add(PosixFilePermission.GROUP_EXECUTE);
        permissions.add(PosixFilePermission.GROUP_READ);
        permissions.add(PosixFilePermission.GROUP_WRITE);

        permissions.add(PosixFilePermission.OWNER_EXECUTE);
        permissions.add(PosixFilePermission.OWNER_READ);
        permissions.add(PosixFilePermission.OWNER_WRITE);

        permissions.add(PosixFilePermission.OTHERS_EXECUTE);
        permissions.add(PosixFilePermission.OTHERS_READ);
        permissions.add(PosixFilePermission.OTHERS_WRITE);
        try {
            return Files.createDirectories(path, PosixFilePermissions.asFileAttribute(permissions));
        } catch (IOException e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        }
        return null;
    }

    //shell和java同是调用
    public static void download(String ip, int port, String username, String password, String localPath,
                                String remoteDir, int maxThread) {
        if (fileServerInfo == null) {
            fileServerInfo = new FileServerInfo(ip, port, username, password, localPath, remoteDir, maxThread, 1);
            File pwd = new File("");
            System.setProperty("SFTP_HOME", pwd.getAbsolutePath());
        }

        //防止java直接调用时,会在错误路径生成log文件
        if (logger == null) {
            logger = LogManager.getLogger(SftpDownload.class);
        }

        SftpUtil.init();
        if (!checkConfig()) {
            return;
        }

        //初始化数据库链接
        if (!DBUtil.init()) {
            return;
        }


        Path localDir = Paths.get(fileServerInfo.getLocalPath());
        if (!Files.exists(localDir)) {
            localDir = mkDirs(localDir);
            if (localDir == null) {
                System.out.printf("time:%s, thread :%s, can't create required directory: %s\n", new Date().toString(),
                        Thread.currentThread().getName(), fileServerInfo.getLocalPath());
                return;
            }
        } else if (!Files.isDirectory(localDir)) {
            System.out.printf("time:%s, thread :%s, local dir: %s isn't a directory\n", new Date().toString(),
                    Thread.currentThread().getName(), localDir.toAbsolutePath().toString());
            return;
        }

        LOCAL = localDir.toAbsolutePath().toString();
        if (!LOCAL.endsWith("/")) {
            LOCAL += "/";
        }

        System.out.printf("time:%s, thread :%s, init sftp connect\n", new Date().toString(),
                Thread.currentThread().getName());

        if (!initSftpConnect()) {
            return;
        }

        Path serverPath = Paths.get(fileServerInfo.getFilePath());
        PREFIX = serverPath.toAbsolutePath().getParent().toString();
        preparedStart(serverPath.getFileName().toString());
        clean();
    }

    public static void remoteInfo(RemoteFile rFile) {
        try {
            Vector<ChannelSftp.LsEntry> ls = globalTransSftp.ls(Paths.get(PREFIX, rFile.getName()).toAbsolutePath().toString());
            for (ChannelSftp.LsEntry entry : ls) {
                rFile.addFile(entry.getFilename(), entry.getAttrs());
            }
        } catch (SftpException e) {
            logger.error("", e);
            System.out.printf("time:%s, thread :%s, something go wrong, reason: %s\n", new Date().toString(),
                    Thread.currentThread().getName(), e.getMessage());
        } catch (Throwable e) {
            logger.error("", e);
        }
    }

    private static void rmLogFile(Map<String, SftpATTRS> files) {
        Iterator<Map.Entry<String, SftpATTRS>> it = files.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, SftpATTRS> entry = it.next();
            String name = entry.getKey();
            if (name.startsWith("log_") && name.endsWith(".txt") || name.equals(uLogName)
                    || name.equals(dLogName) || name.startsWith(".") || name.endsWith(".filepart")) {
                it.remove();
            }
        }
    }

    private static int compareIp(String ip1, String ip2) {
        String[] ips1 = ip1.split("\\.");
        String[] ips2 = ip2.split("\\.");
        int length = ips1.length;
        for (int i = 0; i < length; i++) {
            int val1 = Integer.parseInt(ips1[i]);
            int val2 = Integer.parseInt(ips2[i]);
            if (val1 == val2) {
                continue;
            }
            if (val1 > val2) {
                return 1;
            } else if (val1 < val2) {
                return -1;
            }
        }
        return 0;
    }

    // rFile是目录
    private static void doJob(RemoteFile rFile) {
        List<RemoteFile> rFiles = new LinkedList<>();
        rFiles.add(rFile);
        while (rFiles.size() > 0) {
            boolean isCsv = false;
            rFile = rFiles.remove(0);
            String dirName = rFile.getName();
            if (rFile.getFiles().size() > 0 && rFile.getDirs().size() == 2) {
                String ipStr = dirName.substring(dirName.lastIndexOf("/") + 1);
                if (ipStr.contains(".")) {
                    if (compareIp(ipStr, fileServerInfo.getStartIp()) < 0 || compareIp(ipStr, fileServerInfo.getEndIp()) > 0) {
                        continue;
                    }
                } else {
                    isCsv = true;
                }
            }

            rmLogFile(rFile.getFiles());
            Path localDir = Paths.get(LOCAL, dirName);
            if (!Files.exists(localDir)) {
                localDir = mkDirs(localDir);
            } else if (!Files.isDirectory(localDir)) {
                System.out.printf("time:%s, thread :%s, %s\n", new Date().toString(),
                        Thread.currentThread().getName(), "file " + localDir + "isn't a directory");
                continue;
            }
            Date now = new Date();
            long validTime = now.getTime() / 1000 - 60;

            int fileSize = rFile.getFiles().size();
            if (fileSize > 0) {
                String[] names = new String[fileSize];
                rFile.getFiles().keySet().toArray(names);
                Arrays.sort(names);
                List<String> downloadNames = new ArrayList<>();
                Set<String> ignoreNames = new HashSet<>();
                initDate();
                for (String name : names) {
                    //上传超过一分钟的文件才下载
                    if (rFile.getFiles().get(name).getMTime() > validTime) {
                        continue;
                    }

                    if (isCsv) {
                        int charIndex = name.indexOf("e_cdr") - 1;
                        String ipStr = name.substring(0, charIndex);
                        if (compareIp(ipStr, fileServerInfo.getStartIp()) < 0 || compareIp(ipStr, fileServerInfo.getEndIp()) > 0) {
                            continue;
                        }
                    }

                    //程序启动时就获取了时间 可能会和下一天存在少量重复
                    if (fileServerInfo.isToday() && !name.replaceAll("-", "").contains(todayStr)) {
                        continue;
                    }
                    if (fileServerInfo.isYestoday() && !name.replaceAll("-", "").contains(yesterday)) {
                        continue;
                    }
                    if (fileServerInfo.isHistory() && (name.replaceAll("-", "").contains(todayStr) || name.replaceAll("-", "").contains(yesterday))) {
                        continue;
                    }

                    Path remotePath = Paths.get(PREFIX, dirName, name);
                    String remoteName = remotePath.toAbsolutePath().toString();
                    String tableName = null;
                    if (name.endsWith(".frm") || name.endsWith(".MYI") || name.endsWith(".MYD")) {
                        tableName = name.substring(0, name.length() - 4);
                    }

                    if (tableName != null && ignoreNames.contains(tableName)) {
                        continue;
                    }

                    int modifyTime = DBUtil.get(remoteName);
                    File localFile = new File(localDir.toFile(), name);
                    //需要重新下载
                    if (modifyTime < rFile.getFiles().get(name).getMTime()) {
                        if (localFile.exists()) {
                            if (!localFile.isFile()) {
                                logger.error("file " + localFile.getAbsolutePath() + "isn't a regular file");
                                continue;
                            }
                            if (!localFile.delete()) {
                                logger.error("delete file" + localFile.getAbsolutePath() + "failed");
                            }
                        }
                        if (tableName != null) {//其他机器上已下载
                            for (String suffix : SUFFIX) {
                                if (!rFile.getFiles().containsKey(tableName + suffix)) {
                                    continue;
                                }
                                File rmFile = new File(localDir.toFile(), tableName + suffix);
                                if (rmFile.exists() && DBUtil.get(PREFIX + "/" + dirName + "/" + tableName + suffix) < rFile.getFiles().get(tableName + suffix).getMTime()) {
                                    if (!rmFile.delete()) {
                                        logger.error("delete file" + rmFile.getAbsolutePath() + "failed");
                                    }
                                }
                                if (!downloadNames.contains(tableName + suffix)) {
                                    downloadNames.add(tableName + suffix);
                                }
                            }
                            ignoreNames.add(tableName);
                            continue;
                        }
                    } else if (!localFile.exists()) {
                        //其他机器上有了就不下
                        continue;
                    }
                    downloadNames.add(name);
                }

                for (String name : downloadNames) {
                    Path remotePath = Paths.get(PREFIX, dirName, name);
                    String remoteName = remotePath.toAbsolutePath().toString();
                    Path localPath = Paths.get(LOCAL, dirName, name);
                    boolean shouldRm = false;
                    if (checkTime(name)) {
                        shouldRm = true;
                    }
                    doSftp(remotePath, localPath, shouldRm, rFile.getFiles().get(name).getMTime() + "," + validTime);
                    DBUtil.addRecord(remoteName, rFile.getFiles().get(name).getMTime());
                }
            }

            rFile.getDirs().remove(".");
            rFile.getDirs().remove("..");

            if (rFile.getDirs().size() > 0) {
                for (String name : rFile.getDirs().keySet()) {
                    RemoteFile subDir = new RemoteFile(dirName + "/" + name, rFile.getDirs().get(name));
                    remoteInfo(subDir);
                    rFiles.add(subDir);
                }
            }
        }
    }

    private static void doSftp(Path src, Path dest, boolean shouldRm, String modifyTime) {
        globalTransListen.reset();
        try {
            String srcName = src.toAbsolutePath().toString();
            String destName = dest.toAbsolutePath().toString();
            globalTransSftp.get(srcName, destName, globalTransListen, ChannelSftp.RESUME);
            transRecord(globalTransListen.getTotal(), globalTransListen.getSkip(), globalTransListen.getSum(), srcName, destName, modifyTime);
            if (shouldRm) {
                globalTransSftp.rm(src.toAbsolutePath().toString());
                System.out.printf("time:%s, thread :%s, %s\n", new Date().toString(), Thread.currentThread().getName(),
                        "delete remote file: " + srcName);
            }
        } catch (Throwable e) {
            System.out.printf("time:%s, thread :%s, %s\n", new Date().toString(), Thread.currentThread().getName(),
                    "handle file " + src.toAbsolutePath().toString() + " error, err:" + e.getMessage());
            logger.error("", e);
        }
    }

    private static void transRecord(long total, long skip, long sum, String src, String dest, String time) {
        if (time.equals("")) {
            time = "0,0";
        }
        String[] times = time.split(",");
        if (sum > 0) {
            System.out.printf("time:%s, thread :%s, file name: %s, file length: %d, skip length: %d, read length: %d, filetime:%s, logtime:%s\n", new Date().toString(), Thread.currentThread().getName(), dest, total, skip,
                    sum, times[0], times[1]);
            ConvertCheck.doConvert(dest);
        }
    }

    //初始化几个跟时间有关的变量
    private static void initDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -fileServerInfo.getDay());
        deadline = calendar.getTime();//需要改成0点
        todayStr = format.format(new Date());

        calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        yesterday = format.format(calendar.getTime());

    }

    private static boolean checkConnect() {
        if (SftpUtil.isHostReach(fileServerInfo.getHost(), fileServerInfo.getTimeout())
                && SftpUtil.isHostConnect(fileServerInfo.getHost(), fileServerInfo.getPort())) {
            return true;
        }
        return false;
    }

    private static void preparedStart(String dirName) {
        try {
            SftpATTRS attr = globalTransSftp.stat(fileServerInfo.getFilePath());
            if (!attr.isDir()) {
                System.out.printf("time:%s, thread :%s, remote dir isn't a directory\n", new Date().toString(),
                        Thread.currentThread().getName());
            } else {
                RemoteFile rFile = new RemoteFile(dirName, attr);
                remoteInfo(rFile);
                System.out.printf("time:%s, thread :%s, begin sftp download\n", new Date().toString(),
                        Thread.currentThread().getName());
                doJob(rFile);
            }
        } catch (SftpException e) {
            logger.error("", e);
            System.out.printf("time:%s, thread :%s, read remote dir: %s,err:" + e.getMessage() + "\n",
                    new Date().toString(), Thread.currentThread().getName(), PREFIX + dirName);
        } catch (Throwable e) {
            logger.error("", e);
        }
    }

    private static boolean checkTime(String name) {
        Matcher matcher = pattern.matcher(name);
        if (matcher.matches()) {
            try {
                if (format.parse(matcher.replaceAll("$1")).before(deadline)) {
                    return true;
                }
            } catch (ParseException e) {
                logger.error("", e);
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
        return false;
    }

    private static boolean initSftpConnect() {
        globalTransSftp = SftpUtil.sftpConnect(fileServerInfo.getHost(), fileServerInfo.getPort(),
                fileServerInfo.getAccount(), fileServerInfo.getPassword(), fileServerInfo.getPrivateKey(),
                fileServerInfo.getPassphrase(), fileServerInfo.getTimeout());
        if (globalTransSftp != null) {
            globalTransListen = new SftpProgressMonitorImpl();
            return true;
        }
        return false;
    }

    private static void clean() {
        if (globalTransSftp != null) {
            SftpUtil.disconnected(globalTransSftp);
        }
    }
}

class SftpProgressMonitorImpl implements SftpProgressMonitor {
    private long total;
    private long sum;
    private long skip;
    private boolean init;

    public SftpProgressMonitorImpl() {
        init = true;
    }

    public long getTotal() {
        return this.total;
    }

    public long getSkip() {
        return this.skip;
    }

    public long getSum() {
        return this.sum;
    }

    public void reset() {
        this.total = 0;
        this.sum = 0;
        this.skip = 0;
        this.init = true;
    }

    @Override
    public void init(int op, String src, String dest, long max) {
        total = max;
    }

    @Override
    public boolean count(long count) {
        if (init && count > 0) {
            skip = count;
            count = 0;
        }
        init = false;
        sum += count;
        return true;
    }

    @Override
    public void end() {

    }
}

class RemoteFile {
    private Map<String, SftpATTRS> files;
    private Map<String, SftpATTRS> dirs;
    private SftpATTRS attr;
    private String name;

    public RemoteFile(String name, SftpATTRS attr) {
        this.name = name;
        this.attr = attr;
        if (attr.isDir()) {
            files = new HashMap<>();
            dirs = new HashMap<>();
        }
    }

    public String getName() {
        return this.name;
    }

    public SftpATTRS getAttr() {
        return this.attr;
    }

    public Boolean isDir() {
        return this.attr.isDir();
    }

    public void addFile(String name, SftpATTRS attr) {
        if (attr.isDir()) {
            this.dirs.put(name, attr);
        } else {
            this.files.put(name, attr);
        }
    }

    public Map<String, SftpATTRS> getFiles() {
        return this.files;
    }

    public Map<String, SftpATTRS> getDirs() {
        return this.dirs;
    }
}

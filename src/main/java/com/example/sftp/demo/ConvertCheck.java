package com.example.sftp.demo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class ConvertCheck {
    private static Logger logger = LogManager.getLogger(ConvertCheck.class);
    private static Set<String> SUFFIX = new HashSet<>();

    static {
        SUFFIX.add(".frm");
        SUFFIX.add(".MYD");
        SUFFIX.add(".MYI");
    }

    static void doConvert(String fileName) {
        int splitIndex = fileName.lastIndexOf("/");
        String dirName = fileName.substring(0, splitIndex);
        String name = fileName.substring(splitIndex + 1);

        if (name.endsWith(".csv")) {
            String outpath = getOutpath(dirName, name, true);
            cpFile(fileName, outpath);
        } else if (name.startsWith("wtmp")) {
            String outpath = getOutpath(dirName, name, true);
            handleWtmp(fileName, outpath);
        } else if (name.endsWith(".gz")) {
            GzHandle.doGz(dirName, fileName, name);
        } else if (isMysqlFile(name)) {
            if (canConverted(dirName, name)) {
                doTrans(dirName, name);
            }
        }
    }

    private static boolean isMysqlFile(String fileName) {
        int length = fileName.length();
        if (fileName.charAt(length - 4) == '.') {
            String suffix = fileName.substring(length - 4);
            if (SUFFIX.contains(suffix)) {
                return true;
            }
        }
        return false;
    }

    private static boolean canConverted(String dirName, String name) {
        String rawName = name.substring(0, name.lastIndexOf('.'));
        for (String suffix : SUFFIX) {
            if (name.endsWith(suffix)) {
                continue;
            }
            File checkFile = new File(dirName + File.separator + rawName + suffix);
            if (!checkFile.exists()) {
                return false;
            }
        }
        return true;
    }

    private static void doTrans(String dirName, String name) {
        int index = name.lastIndexOf('.');
        String table = name.substring(0, index);
        name = table + ".mysql";
        String dbTable = "dumpdb." + table;
        String outName = "/var/lib/mysql-files/" + name;
        File to = new File("/var/lib/mysql/dumpdb");
        Set<File> toRemove = new HashSet<File>();
        try {
            emptyDir("/var/lib/mysql-files/", null);
            emptyDir("/var/lib/mysql/dumpdb", "b12_ext_ip_list");
            for (String suffix : SUFFIX) {
                String realName = table + suffix;
                File f = new File(dirName + File.separator + realName);
                File dest = new File(to, realName);
                Files.copy(f.toPath(), dest.toPath());
                dest.setReadable(true, false);
                dest.setWritable(true, false);
                dest.setExecutable(true, false);
                toRemove.add(dest);
            }

            System.out.printf("time:%s, thread :%s, begin to convert file %s \n",
                    new Date().toString(), Thread.currentThread().getName(), dirName + "/" + name);

            if (DBUtil.convert(dbTable, outName)) {
                String dumpPath = getOutpath(dirName, name, true);
                String heads = DBUtil.getHead(table);
                if (heads != null) {
                    Process p = Runtime.getRuntime().exec(new String[]{"sh", "-c", "sed -i '1i\\" + heads + "' " + outName + " && mv " + outName + " " + dumpPath});
                    p.waitFor();
                    System.out.printf("time:%s, thread :%s, convert file %s successed!\n",
                            new Date().toString(), Thread.currentThread().getName(), outName);
                    return;
                }
            }
            System.out.printf("time:%s, thread :%s, convert file %s failed\n",
                    new Date().toString(), Thread.currentThread().getName(), outName);
            toRemove.add(new File(outName));
        } catch (IOException | InterruptedException e) {
            System.out.printf("time:%s, thread :%s, convert file %s failed\n",
                    new Date().toString(), Thread.currentThread().getName(), outName);
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            for (File f : toRemove) {
                f.delete();
            }
        }

        return;
    }

    public static String getOutpath(String dir, String name, boolean create) {
        dir = dir.substring(SftpDownload.fileServerInfo.getLocalPath().length() + 1);
        if (dir.charAt(0) == '/') {
            dir = dir.substring(1);
        }
        if (create) {
            File f = new File(SftpDownload.fileServerInfo.getDumpDir() + File.separator + dir);
            if (!f.exists()) {
                f.mkdirs();
            }
        }
        return SftpDownload.fileServerInfo.getDumpDir() + File.separator + dir + File.separator + name;
    }

    private static boolean cpFile(String src, String dest) {
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"sh", "-c", "cp " + src + " " + dest});
            p.waitFor();
            return true;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            logger.error("", e);
        }
        return false;
    }

    private static boolean handleWtmp(String src, String dest) {
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"sh", "-c", "echo \"username|tty|login_ip|login_time|logout_time|duration\" > " + dest + ";last -f " + src + " >> " + dest});
            p.waitFor();
            return true;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            logger.error("", e);
        }
        return false;
    }

    static void emptyDir(String dir, String ignore) {
        File f = new File(dir);
        String[] names = null;
        if (ignore != null) {
            names = f.list(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.contains(ignore)) {
                        return false;
                    }
                    return true;
                }
            });
        } else {
            names = f.list();
        }
        for (String name : names) {
            new File(f, name).deleteOnExit();
        }
    }
}

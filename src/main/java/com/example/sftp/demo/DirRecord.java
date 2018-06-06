package com.example.sftp.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

public class DirRecord {
    private Logger logger;
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private String dirName;
    private AtomicInteger count = new AtomicInteger();
    private PrintWriter fos;
    private boolean readDone;

    protected DirRecord(String dirName) {
        logger = LogManager.getLogger(DirRecord.class);
        SftpDownload.wg.incrementAndGet();
        this.dirName = dirName;
        getPrint();
    }

    protected synchronized boolean checkFinish(ChannelSftp sftp, SftpProgressMonitorImpl listen, boolean done) {
        int remain = 1;

        if (done) {
            this.readDone = true;
            remain = count.get();
        } else {
            remain = count.decrementAndGet();
        }
        try {
            if (this.readDone && remain == 0) {
                if (fos != null) {
                    fos.flush();
                    fos.close();
                }
                if (fos != null) {
                    fos = null;
                    //if (sftp == null || listen == null) {
                    //sftp = SftpDownload.globalTransSftp;
                    //listen = SftpDownload.globalTransListen;
                    //}
                    //synchronized (sftp) {
                    //	finishJob(sftp, listen);
                    //}
                }
                SftpDownload.dirRecords.remove(dirName);
            }

//		if (!done) {
//			SftpDownload.syncChannel(sftp);
//		}
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            synchronized (SftpDownload.wg) {
                if (this.readDone && remain <= 0) {
                    if (SftpDownload.wg.decrementAndGet() <= 0 && SftpDownload.threadPoolDone) {
                        if (sftp != null) {
                            SftpDownload.syncChannel(sftp);
                        }
                        SftpDownload.wg.notifyAll();
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void finishJob(ChannelSftp sftp, SftpProgressMonitorImpl listen) {
        listen.reset();
        try {

            sftp.put(SftpDownload.LOCAL + dirName + "/download.log", SftpDownload.PREFIX + dirName + "/download.log",
                    listen, ChannelSftp.APPEND);
            transRecord(listen.getTotal(), listen.getSkip(), listen.getSum(), "download.log", "");
            new File(SftpDownload.LOCAL + dirName + "/download.log").deleteOnExit();
        } catch (SftpException e) {
            logger.error("", e);
        }

    }

    private void getPrint() {
        if (fos == null) {
            File log = new File(SftpDownload.LOCAL + dirName + "/download.log");
            if (log.exists()) {
                if (!log.isFile()) {
                    log.delete();
                }
            } else {
                try {
                    new File(SftpDownload.LOCAL + dirName).mkdirs();
                    log.createNewFile();
                } catch (IOException e) {
                    logger.error("", e);
                }
            }
            try {
                fos = new PrintWriter(log);
            } catch (FileNotFoundException e) {
                logger.error("", e);
            }
        }
        return;
    }

    protected void transSkip(String name) {
        //System.out.printf("time:%s, thread :%s, judge by time, file %s is skipped", new Date().toString(), Thread.currentThread().getName(), SftpDownload.PREFIX+dirName+"/"+name);
        if (fos != null) {
            fos.append("judge by time, file " + SftpDownload.PREFIX + dirName + name + " is skipped");
        }
    }

    protected void transSkipByConfig(String name) {
        //System.out.printf("time:%s, thread :%s, judge by time, file %s is skipped", new Date().toString(), Thread.currentThread().getName(), SftpDownload.PREFIX+dirName+"/"+name);
        if (fos != null) {
            fos.append("judge by config, file " + SftpDownload.PREFIX + dirName + name + " is skipped");
        }
    }

    protected void transRecord(long total, long skip, long sum, String name, String time) {
        String message = "";
        if (time.equals("")) {
            time = "0,0";
        }
        String[] times = time.split(",");
        if (sum == 0) {
            //name = SftpDownload.PREFIX + dirName + "/" + name;
            //System.out.printf("time:%s, thread :%s, skip file " + name+" "+times[0]+" "+times[1] + "\n", new Date().toString(), Thread.currentThread().getName());
            //return;
        } else {
            if (!name.equals("download.log")) {
                //DBUtil.addRow(SftpDownload.LOCAL + dirName + "/" + name, total, Long.parseLong(times[0]));
                SftpDownload.finishFiles.info(SftpDownload.LOCAL + dirName + "/" + name);
            }
            message = name + "\t" + format.format(new Date()) + "\t" + total + "\t" + skip + "\t" + sum + " " + times[0] + " " + times[1] + "\n";
            name = SftpDownload.PREFIX + dirName + "/" + name;
            System.out.printf("time:%s, thread :%s, file name: %s, file length: %d, skip length: %d, read length: %d, filetime:%s, logtime:%s\n", new Date().toString(), Thread.currentThread().getName(), name, total, skip,
                    sum, times[0], times[1]);
        }
        if (fos != null) {
            fos.append(message);
        }
    }

    protected void delRecord(String name) {
        String message = "delete remote file: " + SftpDownload.PREFIX + dirName + "/" + name + "\n";
        System.out.printf("time:%s, thread :%s, " + message, new Date().toString(), Thread.currentThread().getName());
        if (fos != null) {
            fos.append(message);
        }
    }

    protected void add() {
        count.incrementAndGet();
    }

}

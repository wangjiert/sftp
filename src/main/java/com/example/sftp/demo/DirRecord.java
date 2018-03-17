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
	private Logger logger = LogManager.getLogger(DirRecord.class);
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String dirName;
	private AtomicInteger count = new AtomicInteger();
	private PrintWriter fos;
	private Boolean readDone;

	protected DirRecord(String dirName) {
		this.dirName = dirName;
		getPrint();
	}

	protected void readDone() {
		this.readDone = true;
		if (count.get() == 0) {
			isDone(true);
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

	protected void transRecord(long total, long skip, long sum, String name) {
		String message = "";
		if (sum == 0) {
			System.out.println("skip file " + name);
			return;
		} else {
			System.out.printf("file name: %s, file length: %d, skip length: %d, read length: %d\n", name, total, skip,
					sum);
			message = name + "\t" + format.format(new Date()) + "\t" + total + "\t" + skip + "\t" + sum + "\n";
		}
		if (fos != null) {
			fos.append(message);
		}
	}

	protected void delRecord(String name) {
		String message = "delete remote file: " + name + "\n";
		System.out.printf(message);
		if (fos != null) {
			fos.append(message);
		}
	}

	protected void add() {
		count.incrementAndGet();
	}

	protected synchronized void isDone(boolean wg) {
		if (wg) {
			if (fos != null) {
				fos.flush();
				fos.close();
				fos = null;
			}
			try {
				SftpDownload.globalListen.reset();
				SftpDownload.globalSftp.put(SftpDownload.LOCAL + dirName + "/download.log",
						SftpDownload.PREFIX + dirName + "/download.log", SftpDownload.globalListen, ChannelSftp.APPEND);
				transRecord(SftpDownload.globalListen.getTotal(), SftpDownload.globalListen.getSkip(),
						SftpDownload.globalListen.getSum(), "download.log");
			} catch (SftpException e) {
				logger.error("", e);
			}

			new File(SftpDownload.LOCAL + dirName + "/download.log").deleteOnExit();
			SftpDownload.dirRecords.remove(dirName);
			return;
		}
		if ( count.decrementAndGet() == 0 && readDone) {
			if (fos != null) {
				fos.flush();
				fos.close();
				fos = null;
			}
			try {
				SftpDownload.globalListen.reset();
				SftpDownload.globalSftp.put(SftpDownload.LOCAL + dirName + "/download.log",
						SftpDownload.PREFIX + dirName + "/download.log", SftpDownload.globalListen, ChannelSftp.APPEND);
				transRecord(SftpDownload.globalListen.getTotal(), SftpDownload.globalListen.getSkip(),
						SftpDownload.globalListen.getSum(), "download.log");
			} catch (SftpException e) {
				logger.error("", e);
			}

			new File(SftpDownload.LOCAL + dirName + "/download.log").deleteOnExit();
			SftpDownload.dirRecords.remove(dirName);
		}
		synchronized (SftpDownload.wg) {
			if (SftpDownload.wg.decrementAndGet() == 0 && SftpDownload.threadPoolDone) {

				SftpDownload.wg.notify();
			}
		}
		SftpDownload.semaphore.release();
	}
}

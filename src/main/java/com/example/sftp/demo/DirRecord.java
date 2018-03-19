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
	private boolean readDone;

	protected DirRecord(String dirName) {
		SftpDownload.wg.incrementAndGet();
		this.dirName = dirName;
		getPrint();
	}

	protected synchronized void checkFinish(ChannelSftp sftp, SftpProgressMonitorImpl listen, boolean done) {
		int remain = 1;

		if (done) {
			this.readDone = true;
			remain = count.get();
		} else {
			remain = count.decrementAndGet();
		}
		if (this.readDone && remain == 0) {
			if (fos != null) {
				fos.flush();
				fos.close();
				fos = null;
			}
			if (fos != null) {
				if (sftp == null || listen == null) {
					sftp = SftpDownload.globalTransSftp;
					listen = SftpDownload.globalTransListen;
				}
				synchronized (sftp) {
					finishJob(sftp, listen);
				}
			}
			SftpDownload.dirRecords.remove(dirName);

			synchronized (SftpDownload.wg) {
				if (SftpDownload.wg.decrementAndGet() == 0 && SftpDownload.threadPoolDone) {
					SftpDownload.syncChannel(sftp);
					SftpDownload.wg.notifyAll();
					return;
				}
			}
		}

		if (!done) {
			SftpDownload.syncChannel(sftp);
		}
	}

	private void finishJob(ChannelSftp sftp, SftpProgressMonitorImpl listen) {
		listen.reset();
		try {
			sftp.put(SftpDownload.LOCAL + dirName + "/download.log", SftpDownload.PREFIX + dirName + "/download.log",
					listen, ChannelSftp.APPEND);
			transRecord(listen.getTotal(), listen.getSkip(), listen.getSum(), "download.log");
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

	protected void transRecord(long total, long skip, long sum, String name) {
		String message = "";

		if (sum == 0) {
			name = SftpDownload.PREFIX + dirName + "/" + name;
			System.out.println("skip file " + name);
			return;
		} else {
			message = name + "\t" + format.format(new Date()) + "\t" + total + "\t" + skip + "\t" + sum + "\n";
			name = SftpDownload.PREFIX + dirName + "/" + name;
			System.out.printf("file name: %s, file length: %d, skip length: %d, read length: %d\n", name, total, skip,
					sum);

		}
		if (fos != null) {
			fos.append(message);
		}
	}

	protected void delRecord(String name) {
		String message = "delete remote file: " + SftpDownload.PREFIX + dirName + "/" + name + "\n";
		System.out.printf(message);
		if (fos != null) {
			fos.append(message);
		}
	}

	protected void add() {
		count.incrementAndGet();
	}

}

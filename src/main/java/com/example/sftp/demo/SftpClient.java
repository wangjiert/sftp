package com.example.sftp.demo;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.example.sftp.model.FileServerInfo;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

public class SftpClient {
	private static Logger logger = LogManager.getLogger(SftpClient.class);
	private static final String CONF_PATH = "conf/conf.properties";
	private static final String LOGNAME= "finish.log";
	private static String HOME_PATH;
	private static FileServerInfo fileServerInfo;
	public static CountDownLatch endSignal;
	private static Semaphore semaphore;
	private static List<ChannelSftp> CHANNELS = new LinkedList<>();
	private static List<File> FILES = new LinkedList<>();
	private static List<SftpUtil.SftpProgressMonitorImpl> LISTENERS = new LinkedList<>();
	private static Task TASK = new Task();
	private static final String UPLOAD = "u";
	private static final String DOWNLOAD = "d";
	
	public static List<ChannelSftp> getChannels() {
		return CHANNELS;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("exec args num must bigger than 2");
			return ;
		}
		if (args[1] == null || args[1].equals("")) {
			System.out.println("the second arg must not be null");
			return ;
		}
		if (args[0] == UPLOAD) {
			upload(args[1]);
		} else if (args[0] == DOWNLOAD) {
			Download.work(args[1]);
		} else {
			System.out.println("the first arg must be u or d");
			return ;
		}
		
	}

	public static void upload(String args) {
		HOME_PATH = System.getProperty("SFTP_HOME");
		if (HOME_PATH == null || HOME_PATH.equals("")) {
			System.out.println("please set system property SFTP_HOME");
			return;
		}

		initFileServerInfo(args);
		if (fileServerInfo == null) {
			return;
		}

		File localDir = new File(fileServerInfo.getLocalPath());
		if (!localDir.exists()) {
			System.out.printf("local dir: %s doesn't exist", localDir.getAbsolutePath());
			return;
		}
		fileServerInfo.setFilePath(fileServerInfo.getFilePath() + "/" + localDir.getName());
		if (fileServerInfo.getFilePath() == null || fileServerInfo.getFilePath().equals("")) {
			System.out.println("remote filepath can't be null");
			return;
		}

		boolean reachable = checkConnect();
		if (!reachable) {
			return;
		}

		initList();
		if (CHANNELS.size() == fileServerInfo.getMax()) {
			semaphore = new Semaphore(fileServerInfo.getMax(), true);
			ExecutorService fixedThreadPool = Executors.newFixedThreadPool(fileServerInfo.getMax());

			int len = FILES.size();
			endSignal = new CountDownLatch(len);
			for (int index = 0; index < len; index++) {
				try {
					semaphore.acquire();
				} catch (InterruptedException e) {
					logger.error(e.getMessage());
					index--;
					continue;
				}
				fixedThreadPool.execute(TASK);
			}

			try {
				endSignal.await();
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				System.out.println("some exception occur, please see log file");
			}
			ChannelSftp channel = CHANNELS.remove(0);
			File logFile = new File(HOME_PATH+LOGNAME);
			SftpUtil.uploadFile(channel, LISTENERS.remove(0), fileServerInfo.getFilePath(), logFile, ChannelSftp.APPEND);
			syncChannel(channel);
			logFile.deleteOnExit();
			fixedThreadPool.shutdown();
		}
		clean();
		System.out.println("finish handle");
	}

	private static void initFileServerInfo(String args) {
		Properties prop = new Properties();
		try (FileInputStream fis = new FileInputStream(HOME_PATH + CONF_PATH);) {
			prop.load(fis);
				prop.setProperty("local.dir", args);
			
			fileServerInfo = new FileServerInfo(prop);
		} catch (FileNotFoundException e) {
			// e.printStackTrace();
			System.out.println("please be sure config file's path is correct");
			return;
		} catch (IOException e) {
			// e.printStackTrace();
			System.out.printf("read file:%s failed, please retry\n", HOME_PATH + "conf/conf.properties");
			return;
		}
	}

	private static void initList() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		forkJoinPool.invoke(new JoinTask());
	}

	private static boolean checkConnect() {
		if (SftpUtil.isHostReach(fileServerInfo.getHost(), fileServerInfo.getTimeout())
				&& SftpUtil.isHostConnect(fileServerInfo.getHost(), fileServerInfo.getPort())) {
			return true;
		}
		return false;
	}

	private static boolean checkRemoteDir(ChannelSftp channel) {
		boolean isDirExist = SftpUtil.isDirExist(channel, fileServerInfo.getFilePath());
		if (!isDirExist) {
			try {
				synchronized (logger) {
					if (!SftpUtil.isDirExist(channel, fileServerInfo.getFilePath())) {
						SftpUtil.makeDir(channel, fileServerInfo.getFilePath());
					}
				}
			} catch (SftpException e) {
				logger.error(e.getMessage());
				return false;
			}
		}
		return true;
	}

	private static void clean() {
		for (ChannelSftp channel : CHANNELS) {
			if (channel.isConnected()) {
				SftpUtil.disconnected(channel);
			}
		}
		CHANNELS.clear();
	}

	private synchronized static File getFile() {
		if (FILES.size() > 0) {
			return FILES.remove(0);
		}
		return null;
	}

	private synchronized static ChannelSftp syncChannel(ChannelSftp channel) {
		if (channel != null) {
			CHANNELS.add(channel);
			return null;
		}
		if (CHANNELS.size() > 0) {
			return CHANNELS.remove(0);
		}
		return null;
	}

	private synchronized static SftpUtil.SftpProgressMonitorImpl syncListen(SftpUtil.SftpProgressMonitorImpl listen) {
		if (listen != null) {
			LISTENERS.add(listen);
			return null;
		}
		if (LISTENERS.size() > 0) {
			return LISTENERS.remove(0);
		}
		return null;
	}

	static class Task implements Runnable {
		public void run() {
			File file = getFile();
			ChannelSftp channel = syncChannel(null);
			SftpUtil.SftpProgressMonitorImpl listen = syncListen(null);

			if (file != null && channel != null && listen != null) {
				SftpUtil.uploadFile(channel, listen, fileServerInfo.getFilePath(), file, ChannelSftp.RESUME);
				syncChannel(channel);
				syncListen(listen);
			}
			semaphore.release();
			SftpClient.endSignal.countDown();
		}
	}

	static class FileTask extends RecursiveAction {

		@Override
		protected void compute() {
			File localDir = new File(fileServerInfo.getLocalPath());
			File files[] = localDir.listFiles(new FileFilter() {

				@Override
				public boolean accept(File pathname) {
					if (pathname.isDirectory()) {
						return false;
					}
					return true;
				}

			});
			FILES.addAll(Arrays.asList(files));
		}

	}

	static class ListTask extends RecursiveAction {
		private int sum;

		public ListTask(int sum) {
			this.sum = sum;
		}

		private void addList(int sum) {
			while (sum > 0) {
				ChannelSftp sftp = SftpUtil.sftpConnect(fileServerInfo.getHost(), fileServerInfo.getPort(),
						fileServerInfo.getAccount(), fileServerInfo.getPassword(), fileServerInfo.getPrivateKey(),
						fileServerInfo.getPassphrase(), fileServerInfo.getTimeout());
				if (sftp != null) {
					if (!checkRemoteDir(sftp)) {
						SftpUtil.disconnected(sftp);
						break;
					}
					SftpUtil.SftpProgressMonitorImpl listen = new SftpUtil.SftpProgressMonitorImpl();
					synchronized (SftpClient.logger) {
						CHANNELS.add(sftp);
						LISTENERS.add(listen);
					}
					sum--;
				}
			}

		}

		@Override
		protected void compute() {
			if (sum > 8) {
				int leftSum = sum / 2;
				ListTask left = new ListTask(leftSum);
				ListTask right = new ListTask(sum - leftSum);
				left.fork();
				right.fork();
				left.join();
				right.join();
			} else {
				addList(sum);
			}
		}

	}

	static class JoinTask extends RecursiveAction {
		@Override
		protected void compute() {
			FileTask left = new FileTask();
			ListTask right = new ListTask(fileServerInfo.getMax());
			left.fork();
			right.fork();
			left.join();
			right.join();
		}

	}
}

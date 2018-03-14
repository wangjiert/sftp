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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.example.sftp.model.FileServerInfo;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

public class SftpClient {
	private static Logger logger = LogManager.getLogger(SftpClient.class);
	private static final String CONF_PATH = "conf/conf.properties";
	private static String HOME_PATH;
	private static FileServerInfo fileServerInfo;
	public static CountDownLatch endSignal;
	private static List<ChannelSftp> CHANNELS = new LinkedList<>();
	private static List<File> FILES = new LinkedList<>();
	private static List<SftpUtil.SftpProgressMonitorImpl> LISTENERS = new LinkedList<>();
	private static Task TASK = new Task();

	public static void main(String[] args) {
		upload(args);
	}

	public static void upload(String[] args) {
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

		boolean reachable = checkConnect();
		if (!reachable) {
			return;
		}

		if (!initList()) {
			return;
		}

		if (!checkRemoteDir()) {
			System.out.printf("can't create required directory: %s, please check log file for reason\n",
					fileServerInfo.getFilePath());
			return;
		}

		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(fileServerInfo.getMax());

		int len = FILES.size();
		endSignal = new CountDownLatch(len);
		for (int index = 0; index < len; index++) {
			fixedThreadPool.execute(TASK);
		}

		try {
			endSignal.await();
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
			System.out.println("some exception occur, please see log file");
		}
		fixedThreadPool.shutdown();
		clean();
		System.out.println("finish handle");
	}

	private static void initFileServerInfo(String args[]) {
		Properties prop = new Properties();
		try (FileInputStream fis = new FileInputStream(HOME_PATH + CONF_PATH);) {
			prop.load(fis);
			if (args.length > 0) {
				prop.setProperty("local.dir", args[0]);
			}
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

	private static boolean initList() {
		ForkJoinPool forkJoinPool = new ForkJoinPool();
		Future<String> result = forkJoinPool.submit(new JoinTask());
		try {
			result.get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			System.out.println("init failed");
			return false;
		}
		return true;
	}

	private static boolean checkConnect() {
		if (SftpUtil.isHostReach(fileServerInfo.getHost(), fileServerInfo.getTimeout())
				&& SftpUtil.isHostConnect(fileServerInfo.getHost(), fileServerInfo.getPort())) {
			return true;
		}
		return false;
	}

	private static boolean checkRemoteDir() {
		fileServerInfo.setFilePath(fileServerInfo.getFilePath().replaceAll("\\\\", "/"));
		boolean isDirExist = SftpUtil.isDirExist(CHANNELS.get(0), fileServerInfo.getFilePath());
		if (!isDirExist) {
			try {
				SftpUtil.makeDir(CHANNELS.get(0), fileServerInfo.getFilePath());
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
				SftpUtil.uploadFile(channel, listen, fileServerInfo.getFilePath(), file);
				syncChannel(channel);
				syncListen(listen);
			}
			SftpClient.endSignal.countDown();
		}
	}

	static class FileTask extends RecursiveTask<Void> {

		@Override
		protected Void compute() {
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
			return null;
		}

	}

	static class ListTask extends RecursiveTask<Void> {
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
					SftpUtil.SftpProgressMonitorImpl listen = new SftpUtil.SftpProgressMonitorImpl();
					synchronized (SftpClient.endSignal) {
						CHANNELS.add(sftp);
						LISTENERS.add(listen);
					}
					sum--;
				}
			}
		}

		@Override
		protected Void compute() {
			if (sum > 8) {
				int leftSum = sum / 2;
				RecursiveTask left = new ListTask(leftSum);
				RecursiveTask right = new ListTask(sum - leftSum);
				left.fork();
				right.fork();
				left.join();
				right.join();
				return null;
			}
			addList(sum);
			return null;
		}

	}

	static class JoinTask extends RecursiveTask<String> {
		@Override
		protected String compute() {
			RecursiveTask left = new FileTask();
			RecursiveTask right = new ListTask(fileServerInfo.getMax());
			left.fork();
			right.fork();
			left.join();
			right.join();
			return "ok";
		}

	}

	public static void download(String[] args) {
		// String sftpHost = prop.getProperty("remote.host");
		// Integer sftpPort = Integer.parseInt(prop.getProperty("remote.port"));
		// String sftpUserName = prop.getProperty("remote.username");
		// String sftpPassword = prop.getProperty("remote.passwd");
		// String remoteFilePath = prop.getProperty("remote.dir");
		// String localFilePath = prop.getProperty("local.dir");
		// String fileName = args[1];
		//
		// FileServerInfo fileServerInfo = new FileServerInfo();
		// fileServerInfo.setDomain(null);
		// fileServerInfo.setHost(sftpHost);
		// fileServerInfo.setPort(sftpPort);
		// fileServerInfo.setAccount(sftpUserName);
		// fileServerInfo.setPassword(sftpPassword);
		// fileServerInfo.setFilePath(remoteFilePath);
		// fileServerInfo.setLocalPath(localFilePath);
		// fileServerInfo.setFileName(fileName);
		//
		// SftpUtil.download(fileServerInfo);
		// System.out.println("下载完成，请在"+localFilePath+"目录下查看！");
	}
}

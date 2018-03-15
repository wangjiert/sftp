package com.example.sftp.demo;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.example.sftp.demo.SftpClient.FileTask;
import com.example.sftp.demo.SftpClient.JoinTask;
import com.example.sftp.demo.SftpClient.ListTask;
import com.example.sftp.demo.SftpClient.Task;
import com.example.sftp.model.FileServerInfo;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

public class Download {
	public static Logger logger = LogManager.getLogger(Download.class);
	private static final String CONF_PATH = "conf/conf.properties";
	private static final String LOGNAME= "download.log";
	public static  String PREFIX = "";
	public static String LOCAL = "";// 需要以/结尾
	private static String HOME_PATH;
	private static Pattern pattern = Pattern.compile("\\D*(\\d{8})\\D*");
	private static List<SftpUtil.SftpProgressMonitorImpl> LISTENERS = new LinkedList<>();
	private static List<ChannelSftp> CHANNELS = new LinkedList<>();
	private static List<RemoteFile> FILES = new LinkedList<>();
	private static FileServerInfo fileServerInfo;
	public static CountDownLatch endSignal;
	private static Semaphore semaphore;
	private static String deadline;
	private static Task TASK = new Task();
	private static ExecutorService fixedThreadPool;
	private static void initDate() {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1);
		deadline = format.format(calendar.getTime());
	}
	
	private static void initFileServerInfo(String args) {
		Properties prop = new Properties();
		try (FileInputStream fis = new FileInputStream(HOME_PATH + CONF_PATH);) {
			prop.load(fis);
			prop.setProperty("remote.dir", args);
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
	
	private static boolean init(String args) {
		HOME_PATH = System.getProperty("SFTP_HOME");
		if (HOME_PATH == null || HOME_PATH.equals("")) {
			System.out.println("please set system property SFTP_HOME");
			return false;
		}
		
		initFileServerInfo(args);
		if (fileServerInfo == null) {
			return false;
		}

		File localDir = new File(fileServerInfo.getLocalPath());
		if (!localDir.exists()) {
			if (!localDir.mkdirs()) {
				System.out.printf("can't create required directory: %s", localDir.getAbsolutePath());
				return false;
			}
		} else if (!localDir.isDirectory()) {
			System.out.printf("local dir: %s isn't a directory", localDir.getAbsolutePath());
			return false;
		}
		
		LOCAL = localDir.getAbsolutePath() + "/";
		
		if (fileServerInfo.getFilePath() == null || fileServerInfo.getFilePath().equals("")) {
			System.out.println("remote filepath can't be null");
			return false;
		}

		boolean reachable = checkConnect();
		if (!reachable) {
			return false;
		}

		ForkJoinPool forkJoinPool = new ForkJoinPool();
		forkJoinPool.invoke(new ListTask(fileServerInfo.getMax()));
		
		if (CHANNELS.size() == fileServerInfo.getMax()) {
			semaphore = new Semaphore(fileServerInfo.getMax(), true);
			fixedThreadPool = Executors.newFixedThreadPool(fileServerInfo.getMax());
		}else {
			clean();
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
	
	private static void work(String args) {
		initDate();
		
		if (!init(args)) {
			return;
		}
		int index = fileServerInfo.getFilePath().lastIndexOf("/");
		PREFIX = fileServerInfo.getFilePath().substring(0, index+1);
		SftpATTRS stat;
		try {
			stat = CHANNELS.get(0).stat(fileServerInfo.getFilePath());
		} catch (SftpException e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			return;
		}
		FILES.add(new RemoteFile(fileServerInfo.getFilePath().substring(index+1), stat));
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
			return;
		}
		fixedThreadPool.submit(TASK);
	}

	// 第一次调用之前需要保证PREFIX+remoteName是目录
	// PREFIX需要有/后缀
	// localPath需要/后缀
	public void jump(ChannelSftp channel, RemoteFile rFile) {
		RemoteFile rFile = remoteInfo(channel, rFile);
		if (rFile == null) {
			return;
		}
		if (rFile.isDir()) {
			File root = new File(LOCAL + remoteName);
			if (!root.exists()) {
				root.mkdir();
			}
			if (rFile.getFiles().containsKey("finish.log")) {
				SftpATTRS logStat = rFile.getFiles().remove("finish.log");
				for (String name : rFile.getFiles().keySet()) {
					if (checkTime(name)) {
						doDelete(remoteName+"/"+name);
						continue;
					}
					SftpATTRS fileStat = rFile.getFiles().get(name);
					if (fileStat.getMTime() >= logStat.getMTime()) {
						continue;
					}
					File localFile = new File(root, name);
					if (localFile.exists() && localFile.lastModified() / 1000 < fileStat.getMTime()) {
						localFile.delete();
					}
					doDownload(channel, remoteName + "/" + name);
				}
			}
			for (String name : rFile.getDirs().keySet()) {
				download(channel, rFile.getDirs().get(name), remoteName + "/" + name);
			}
		} else {
			synchronized(logger) {
				FILES.add(rFile);
			}
		}
	}

	private void doDelete(String filepath) {
		
	}
	
	private boolean checkTime(String name) {
		Matcher matcher = pattern.matcher(name);
		if (matcher.matches()) {
			if (matcher.replaceAll("$1").compareTo(deadline) == -1 ) {
				return true;
			}
		}
		return false;
	}

	public RemoteFile remoteInfo(ChannelSftp sftp, String filepath) {
		try {
			SftpATTRS attr = sftp.stat(PREFIX+filepath);
			RemoteFile rFile = new RemoteFile(filepath, attr);
			if (attr.isDir()) {
				Vector<ChannelSftp.LsEntry> ls = sftp.ls(filepath);
				for (ChannelSftp.LsEntry entry : ls) {
					rFile.addFile(entry.getFilename(), entry.getAttrs());
				}
			}
			return rFile;

		} catch (SftpException e) {
			logger.error(e.getMessage());
			if (e.getMessage().contains("No such file")) {
				System.out.printf("remote dir: %s doesn't exist\n", filepath);
			} else {
				System.out.printf("something go wrong, reason: %s\n", e.getMessage());
			}
			return null;
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
					SftpUtil.SftpProgressMonitorImpl listen = new SftpUtil.SftpProgressMonitorImpl();
					synchronized (Download.logger) {
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
	
	private synchronized static RemoteFile getFile() {
		if (FILES.size() > 0) {
			return FILES.remove(0);
		}
		return null;
	}
	static class Task implements Runnable {
		public void run() {
			RemoteFile file = getFile();
			ChannelSftp channel = syncChannel(null);
			SftpUtil.SftpProgressMonitorImpl listen = syncListen(null);

			if (file != null && channel != null && listen != null) {
				download(channel, file.getAttr(), file.getName());
				syncChannel(channel);
				syncListen(listen);
			}
			semaphore.release();
			SftpClient.endSignal.countDown();
		}
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
	private static void clean() {
		for (ChannelSftp channel : CHANNELS) {
			if (channel.isConnected()) {
				SftpUtil.disconnected(channel);
			}
		}
		CHANNELS.clear();
	}
}

class SftpProgressMonitorImpl implements SftpProgressMonitor {
	private long total;
	private long sum;
	private long skip;
	private boolean init;
	private boolean append;
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

	private void reset() {
		this.total = 0;
		this.sum = 0;
		this.skip = 0;
		this.init = true;
	}
	
	@Override
	public void init(int op, String src, String dest, long max) {
		total = max;
		if (src.contains("download.log")) {
			append= true;
		}
	}

	@Override
	public boolean count(long count) {
		if (!append && init && count > 0) {
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


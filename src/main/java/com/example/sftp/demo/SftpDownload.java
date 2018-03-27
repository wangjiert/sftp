package com.example.sftp.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.example.sftp.model.FileServerInfo;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

public class SftpDownload {
	private static Logger logger;
	protected static boolean threadPoolDone;
	protected static ChannelSftp globalTransSftp;
	protected static SftpProgressMonitorImpl globalTransListen;
	private static ChannelSftp globalStatSftp;
	private static SftpProgressMonitorImpl globalListen;
	private static String HOME_PATH;
	private static final String CONF_PATH = "conf/conf.properties";
	private static final String dLogName = "download.log";
	private static final String uLogName = "finish.log";
	public static String PREFIX = "";
	protected static String LOCAL = "";
	private static Pattern pattern = Pattern.compile("\\D*(\\d{8})\\D*");
	private static List<SftpProgressMonitorImpl> LISTENERS = new LinkedList<>();
	private static List<ChannelSftp> CHANNELS = new LinkedList<>();

	private static FileServerInfo fileServerInfo;
	protected static Semaphore semaphore;

	private static String deadline;
	private static ExecutorService fixedThreadPool;
	protected static AtomicInteger wg = new AtomicInteger();
	protected static Map<String, DirRecord> dirRecords = new HashMap<>();

	public static void main(String[] args) {
		 //download("192.168.130.201", 22, "test", "123456", "/home/arch/Downloads",
		 //"/tmp/upload1", 10);
		System.out.println("=============================================================================");
		System.out.printf("=========begin new transmission======time:%s====\n", new Date().toString());
		System.out.println("=============================================================================");
		if (args[1].equals("u")) {
			SftpUpload.upload(args);
			return;
		}
		HOME_PATH = System.getProperty("SFTP_HOME");
		logger = LogManager.getLogger(SftpDownload.class);
		File conf = new File(HOME_PATH+"../conf/my.conf");
		if (conf.exists()) {
			parseConfig(conf.getAbsolutePath());
		} else {
			initFileServerInfo(args);
		}
		if (fileServerInfo == null) {
			return;
		}
		download(fileServerInfo.getHost(), fileServerInfo.getPort(), fileServerInfo.getAccount(),
				fileServerInfo.getPassword(), fileServerInfo.getLocalPath(), args[0], fileServerInfo.getMax());
		
		System.out.println("=============================================================================");
		System.out.printf("=========end new transmission=======time:%s=======\n", new Date().toString());
		System.out.println("=============================================================================");
	}

	private static void parseConfig(String filename) {
		Pattern pattern = Pattern.compile("^\\[.*\\]$");
        try (
        		BufferedReader f = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
        		){
			while(true) {
				String line = f.readLine().trim();
				if (line == null) {
					return;
				} else if (line.trim().equals("[mydump]")) {
					break;
				} 
			}
			Properties properties = new Properties();
			while (true) {
				String line = f.readLine().trim();
				if (line == null) {
					break;
				} else if (line.startsWith("#")) {
					continue;
				} else if (line.equals("")) {
					continue;
				} else if (pattern.matcher(line).matches()){
					break;
				} else {
					String[] lines = line.split("=");
					if (lines.length != 2) {
						continue;
					}
					properties.setProperty(lines[0], lines[1]);
				}
			}
			fileServerInfo = new FileServerInfo(properties);
		} catch (FileNotFoundException e) {
			logger.error("", e);
		} catch (IOException e) {
			logger.error("", e);
		}
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
			System.out.printf("time:%s, thread :%s, please be sure config file's path is correct\n", new Date().toString(), Thread.currentThread().getName());
			return;
		} catch (IOException e) {
			logger.error("", e);
			System.out.printf("time:%s, thread :%s, read file:%s failed, please retry\n", new Date().toString(), Thread.currentThread().getName(), HOME_PATH + "conf/conf.properties");
			return;
		}
	}

	public static void download(String ip, int port, String username, String password, String localPath,
			String remoteDir, int maxThread) {
		if (fileServerInfo == null) {
			fileServerInfo = new FileServerInfo(ip, port, username, password, localPath, remoteDir, maxThread, 1);
			File pwd = new File("");
			System.setProperty("SFTP_HOME", pwd.getAbsolutePath());
		}
		if (logger == null) {
			logger = LogManager.getLogger(SftpDownload.class);
		}
		SftpUtil.init();
		initDate();

		File localDir = new File(fileServerInfo.getLocalPath());
		if (!localDir.exists()) {
			if (!localDir.mkdirs()) {
				System.out.printf("time:%s, thread :%s, can't create required directory: %s\n", new Date().toString(), Thread.currentThread().getName(), localDir.getAbsolutePath());
				return;
			}
		} else if (!localDir.isDirectory()) {
			System.out.printf("time:%s, thread :%s, local dir: %s isn't a directory\n", new Date().toString(), Thread.currentThread().getName(), localDir.getAbsolutePath());
			return;
		}

		LOCAL = localDir.getAbsolutePath();
		if (!LOCAL.endsWith("/")) {
			LOCAL += "/";
		}

		if (fileServerInfo.getFilePath() == null || fileServerInfo.getFilePath().equals("")) {
			System.out.printf("time:%s, thread :%s, remote filepath can't be null\n", new Date().toString(), Thread.currentThread().getName());
			return;
		}

		boolean reachable = checkConnect();
		if (!reachable) {
			return;
		}

		ForkJoinPool forkJoinPool = new ForkJoinPool();
		forkJoinPool.invoke(new ListTask(fileServerInfo.getMax() + 2));
		if (CHANNELS.size() == fileServerInfo.getMax() + 2) {
			globalTransSftp = CHANNELS.remove(0);
			globalTransListen = LISTENERS.remove(0);
			globalStatSftp = CHANNELS.remove(0);
			LISTENERS.remove(0);
			semaphore = new Semaphore(fileServerInfo.getMax() * 2, true);
			fixedThreadPool = Executors.newFixedThreadPool(fileServerInfo.getMax());
		} else {
			clean();
			return;
		}
		int index = fileServerInfo.getFilePath().lastIndexOf("/");
		PREFIX = fileServerInfo.getFilePath().substring(0, index + 1);
		preparedStart(fileServerInfo.getFilePath().substring(index + 1));

		try {
			synchronized (wg) {
				threadPoolDone = true;
				if (wg.get() > 0) {
					wg.wait();
				}
			}
		} catch (InterruptedException e) {
			logger.error("", e);
		}
		fixedThreadPool.shutdown();
		clean();
	}

	public static void remoteInfo(RemoteFile rFile) {
		try {
			Vector<ChannelSftp.LsEntry> ls = globalStatSftp.ls(PREFIX + rFile.getName());
			for (ChannelSftp.LsEntry entry : ls) {
				rFile.addFile(entry.getFilename(), entry.getAttrs());
			}
		} catch (SftpException e) {
			logger.error("", e);
			System.out.printf("time:%s, thread :%s, something go wrong, reason: %s\n", new Date().toString(), Thread.currentThread().getName(), e.getMessage());
		}
	}

	// rFile是目录
	private static void doJob(RemoteFile rFile) {
		List<RemoteFile> rFiles = new LinkedList<>();
		rFiles.add(rFile);
		while (rFiles.size() > 0) {
			rFile = rFiles.remove(0);
			String dirName = rFile.getName();
			if (rFile.getFiles().containsKey(uLogName)) {
				File localDir = new File(LOCAL + dirName);
				if (!localDir.exists()) {
					localDir.mkdirs();
				} else if (!localDir.isDirectory()) {
					String errMessage = "file " + localDir + "isn't a directory";
					logger.error(errMessage);
					System.out.printf("time:%s, thread :%s, "+errMessage+"\n", new Date().toString(), Thread.currentThread().getName());
					continue;
				}
				SftpATTRS LogAttr = rFile.getFiles().remove(uLogName);
				rFile.getFiles().remove(dLogName);

				int fileSize = rFile.getFiles().size();
				DownloadTask downloadTask = null;
				if (fileSize > 0) {
					downloadTask = new DownloadTask(fileSize, dirName);
					DirRecord dirRecord = new DirRecord(dirName);
					dirRecords.put(dirName, dirRecord);

					for (String name : rFile.getFiles().keySet()) {

						if (rFile.getFiles().get(name).getMTime() > LogAttr.getMTime()) {
							dirRecord.transSkip(name);
							continue;
						}
						File localFile = new File(localDir, name);
						if (localFile.exists()) {
							if (!localFile.isFile()) {
								logger.error("file " + localFile.getAbsolutePath() + "isn't a regular file");
								continue;
							}
							if (localFile.lastModified() / 1000 < rFile.getFiles().get(name).getMTime()) {
								localFile.delete();
							}
						}
						if (checkTime(name)) {
							downloadTask.addDelFile(name);
						}
						try {
							semaphore.acquire();
							downloadTask.addTimes(name, rFile.getFiles().get(name).getMTime(), LogAttr.getMTime());
							downloadTask.addFile(name);
							fixedThreadPool.submit(downloadTask);

						} catch (InterruptedException e) {
							logger.error("", e);
							continue;
						}
					}
					downloadTask = null;
				}
			}
			if (dirRecords.get(dirName) != null) {
				dirRecords.get(dirName).checkFinish(null, null, true);
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

	static class DownloadTask implements Runnable {
		private String dirName;
		private List<String> files = new LinkedList<>();
		private Set<String> delFiles = new HashSet<>();
		private Map<String, String> times = new HashMap<>();
		
		protected synchronized void addTimes(String name, long file, long log) {
			times.put(name, file+","+log);
		}
		
		private synchronized String getTimes(String name) {
			return times.remove(name);
		}
		
		public synchronized void addFile(String name) {
			dirRecords.get(dirName).add();
			files.add(name);
		}

		protected synchronized void addDelFile(String name) {
			this.delFiles.add(name);
		}

		private synchronized boolean isDeleted(String name) {
			if (delFiles.contains(name)) {
				delFiles.remove(name);
				return true;
			}
			return false;
		}

		private synchronized String getFile() {
			return files.remove(0);
		}

		public DownloadTask(int count, String dirName) {
			this.dirName = dirName;
		}

		private void doSftp(ChannelSftp sftp, SftpProgressMonitorImpl listen, DirRecord dirRecord, String name)
				throws SftpException {
			sftp.get(PREFIX + dirName + "/" + name, LOCAL + dirName + "/" + name, listen, ChannelSftp.RESUME);
			dirRecord.transRecord(listen.getTotal(), listen.getSkip(), listen.getSum(), name, getTimes(name));
			if (isDeleted(name)) {
				sftp.rm(SftpDownload.PREFIX + dirName + "/" + name);
				dirRecord.delRecord(name);
			}
		}

		@Override
		public void run() {
			ChannelSftp sftp = syncChannel(null);
			SftpProgressMonitorImpl listen = syncListen(null);

			String name = getFile();
			listen.reset();
			try {
				doSftp(sftp, listen, dirRecords.get(dirName), name);
			} catch (SftpException e) {
				if (e.getMessage().equals("failed to resume for ")) {
					new File(LOCAL + dirName + "/" + name).deleteOnExit();
					try {
						doSftp(sftp, listen, dirRecords.get(dirName), name);
					} catch (SftpException e1) {
						logger.error("", e1);
					}
				} else {
					logger.error(name, e);
				}
			}

			dirRecords.get(dirName).checkFinish(sftp, listen, false);
			syncListen(listen);
			
			semaphore.release();
		}
	}

	private static void initDate() {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -fileServerInfo.getDay());
		deadline = format.format(calendar.getTime());
	}

	private static boolean checkConnect() {
		if (SftpUtil.isHostReach(fileServerInfo.getHost(), fileServerInfo.getTimeout())
				&& SftpUtil.isHostConnect(fileServerInfo.getHost(), fileServerInfo.getPort())) {
			return true;
		}
		return false;
	}

	private static void preparedStart(String dirName) {
		ChannelSftp sftp = syncChannel(null);
		try {
			SftpATTRS attr = sftp.stat(PREFIX + dirName);
			if (!attr.isDir()) {
				System.out.printf("time:%s, thread :%s, remote dir isn't a directory\n", new Date().toString(), Thread.currentThread().getName());
			} else {
				RemoteFile rFile = new RemoteFile(dirName, attr);
				Vector<ChannelSftp.LsEntry> ls = sftp.ls(PREFIX + dirName);
				syncChannel(sftp);
				sftp = null;
				for (ChannelSftp.LsEntry entry : ls) {
					rFile.addFile(entry.getFilename(), entry.getAttrs());
				}
				doJob(rFile);
			}
		} catch (SftpException e) {
			logger.error("", e);
			System.out.printf("time:%s, thread :%s, "+e.getMessage(), new Date().toString(), Thread.currentThread().getName());
		}
		if (sftp != null) {
			syncChannel(sftp);
		}
	}

	private static boolean checkTime(String name) {
		Matcher matcher = pattern.matcher(name);
		if (matcher.matches()) {
			if (matcher.replaceAll("$1").compareTo(deadline) < 0) {
				return true;
			}
		}
		return false;
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
					SftpProgressMonitorImpl listen = new SftpProgressMonitorImpl();
					synchronized (SftpDownload.logger) {
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

	protected synchronized static ChannelSftp syncChannel(ChannelSftp channel) {
		if (channel != null) {
			CHANNELS.add(channel);
			return null;
		}
		if (CHANNELS.size() > 0) {
			return CHANNELS.remove(0);
		}
		return null;
	}

	private synchronized static SftpProgressMonitorImpl syncListen(SftpProgressMonitorImpl listen) {
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
		if (globalTransSftp != null) {
			SftpUtil.disconnected(globalTransSftp);
		}
		if (globalStatSftp != null) {
			SftpUtil.disconnected(globalStatSftp);
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

	public void reset() {
		this.total = 0;
		this.sum = 0;
		this.skip = 0;
		this.init = true;
	}

	@Override
	public void init(int op, String src, String dest, long max) {
		total = max;
		if (src.contains("download.log")) {
			append = true;
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

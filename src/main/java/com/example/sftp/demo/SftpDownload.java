package com.example.sftp.demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;

import com.example.sftp.model.FileServerInfo;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

public class SftpDownload {
	public static Logger logger;
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String HOME_PATH;
	private static final String CONF_PATH = "conf/conf.properties";
	private static final String dLogName = "download.log";
	private static final String uLogName = "finish.log";
	public static String PREFIX = "";
	public static String LOCAL = "";
	private static Pattern pattern = Pattern.compile("\\D*(\\d{8})\\D*");
	private static List<SftpProgressMonitorImpl> LISTENERS = new LinkedList<>();
	private static List<ChannelSftp> CHANNELS = new LinkedList<>();

	private static FileServerInfo fileServerInfo;
	private static Semaphore semaphore;

	private static String deadline;
	private static ExecutorService fixedThreadPool;
	private static DeleteTask deleteTask = new DeleteTask();
	private static AtomicInteger wg = new AtomicInteger();

	public static void main(String[] args) {
		// download("192.168.130.201", 22, "test", "123456", "/home/arch/Downloads",
		// "/tmp/upload1", 10);
		HOME_PATH = System.getProperty("SFTP_HOME");
		initFileServerInfo(args);
		if (fileServerInfo == null) {
			return;
		}
		download(fileServerInfo.getHost(), fileServerInfo.getPort(), fileServerInfo.getAccount(),
				fileServerInfo.getPassword(), fileServerInfo.getLocalPath(), args[0], fileServerInfo.getMax());
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
			// e.printStackTrace();
			System.out.println("please be sure config file's path is correct");
			return;
		} catch (IOException e) {
			// e.printStackTrace();
			System.out.printf("read file:%s failed, please retry\n", HOME_PATH + "conf/conf.properties");
			return;
		}
	}

	public static void download(String ip, int port, String username, String password, String localPath,
			String remoteDir, int maxThread) {
		if (fileServerInfo == null) {
			fileServerInfo = new FileServerInfo(ip, port, username, password, localPath, remoteDir, maxThread);
			File pwd = new File("");
			System.setProperty("SFTP_HOME", pwd.getAbsolutePath());
		}
		logger = LogManager.getLogger(SftpDownload.class);
		initDate();

		File localDir = new File(fileServerInfo.getLocalPath());
		if (!localDir.exists()) {
			if (!localDir.mkdirs()) {
				System.out.printf("can't create required directory: %s", localDir.getAbsolutePath());
				return;
			}
		} else if (!localDir.isDirectory()) {
			System.out.printf("local dir: %s isn't a directory", localDir.getAbsolutePath());
			return;
		}

		LOCAL = localDir.getAbsolutePath();
		if (!LOCAL.endsWith("/")) {
			LOCAL += "/";
		}

		if (fileServerInfo.getFilePath() == null || fileServerInfo.getFilePath().equals("")) {
			System.out.println("remote filepath can't be null");
			return;
		}

		boolean reachable = checkConnect();
		if (!reachable) {
			return;
		}

		ForkJoinPool forkJoinPool = new ForkJoinPool();
		forkJoinPool.invoke(new ListTask(fileServerInfo.getMax()));

		if (CHANNELS.size() == fileServerInfo.getMax()) {
			semaphore = new Semaphore(fileServerInfo.getMax(), true);
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
				if (wg.get() > 0) {
					wg.wait();
				}
			}
		} catch (InterruptedException e) {
			logger.error("", e);
		}
		fixedThreadPool.shutdown();
		clean();
		System.out.println("finish handle");
	}

	public static void remoteInfo(ChannelSftp sftp, RemoteFile rFile) {
		try {
			Vector<ChannelSftp.LsEntry> ls = sftp.ls(PREFIX + rFile.getName());
			for (ChannelSftp.LsEntry entry : ls) {
				rFile.addFile(entry.getFilename(), entry.getAttrs());
			}
		} catch (SftpException e) {
			logger.error("", e);
			System.out.printf("something go wrong, reason: %s\n", e.getMessage());
		}
	}

	// rFile是目录
	private static void doJob(RemoteFile rFile) {
		String dirName = rFile.getName();
		if (rFile.getFiles().containsKey(uLogName)) {
			File localDir = new File(LOCAL + dirName);
			if (!localDir.exists()) {
				localDir.mkdir();
			} else if (!localDir.isDirectory()) {
				String errMessage = "file " + localDir + "isn't a directory";
				logger.error(errMessage);
				System.out.println(errMessage);
				return;
			}
			SftpATTRS LogAttr = rFile.getFiles().remove(uLogName);
			rFile.getFiles().remove(dLogName);

			int fileSize = rFile.getFiles().size();
			DownloadTask downloadTask = null;
			if (fileSize > 0) {
				downloadTask = new DownloadTask(fileSize, dirName);
			}
			for (String name : rFile.getFiles().keySet()) {

				try {
					if (checkTime(name)) {
						semaphore.acquire();
						deleteTask.addFile(PREFIX + dirName + "/" + name);
						fixedThreadPool.submit(deleteTask);
						continue;
					}
					if (rFile.getFiles().get(name).getMTime() >= LogAttr.getMTime()) {
						continue;
					}

					File localFile = new File(localDir, name);
					if (localFile.exists()) {
						if (!localFile.isFile()) {
							logger.error("file " + localFile.getAbsolutePath() + "isn't a regular file");
							continue;
						}
						if (localFile.lastModified() / 1000 < rFile.getAttr().getMTime()) {
							localFile.delete();
						}
					}
					semaphore.acquire();
					downloadTask.addFile(name);
					fixedThreadPool.submit(downloadTask);
				} catch (InterruptedException e) {
					logger.error("", e);
					continue;
				}
			}
			downloadTask = null;
		}
		rFile.getDirs().remove(".");
		rFile.getDirs().remove("..");
		if (rFile.getDirs().size() > 0) {

			for (String name : rFile.getDirs().keySet()) {

				RemoteFile subDir = new RemoteFile(dirName + "/" + name, rFile.getDirs().get(name));
				try {
					semaphore.acquire();
					ChannelSftp sftp = syncChannel(null);
					remoteInfo(sftp, subDir);
					syncChannel(sftp);
					semaphore.release();
				} catch (InterruptedException e) {
					logger.error("", e);
					continue;
				}
				doJob(subDir);
			}
		}
	}

	static class DownloadTask implements Runnable {
		private AtomicInteger count = new AtomicInteger();
		private String dirName;
		private PrintWriter fos;
		private List<String> files = new LinkedList<>();

		public synchronized void addFile(String name) {
			count.incrementAndGet();
			wg.incrementAndGet();
			files.add(name);
		}

		private synchronized String getFile() {
			return files.remove(0);
		}

		private synchronized void log(long total, long skip, long sum, String name) {
			String message = "";
			if (sum == 0) {
				System.out.println("skip file " + name);
				return;
			} else {
				System.out.printf("file name: %s, file length: %d, skip length: %d, read length: %d\n", name, total,
						skip, sum);
				message = name + "\t" + format.format(new Date()) + "\t" + total + "\t" + skip + "\t" + sum + "\n";
			}
			if (fos != null) {
				fos.append(message);
			}
		}

		private void done() {
			if (fos != null) {
				fos.flush();
				fos.close();
				fos = null;
			}
		}

		public DownloadTask(int count, String dirName) {
			this.dirName = dirName;
			if (count > 0) {
				try {
					File log = new File(LOCAL + dirName + "/" + dLogName);
					if (log.exists()) {
						if (!log.isFile()) {
							log.delete();
						}
					} else {
						log.createNewFile();
					}
					fos = new PrintWriter(log);

				} catch (IOException e) {
					logger.error("", e);
				}
			}
		}

		@Override
		public void run() {
			if (files.size() > 0 && CHANNELS.size() > 0 && LISTENERS.size() > 0) {
				ChannelSftp sftp = syncChannel(null);
				SftpProgressMonitorImpl listen = syncListen(null);

				String name = getFile();
				listen.reset();
				try {
					sftp.get(PREFIX + dirName + "/" + name, LOCAL + dirName + "/" + name, listen, ChannelSftp.RESUME);
					log(listen.getTotal(), listen.getSkip(), listen.getSum(), name);
				} catch (SftpException e) {
					if (e.getMessage().equals("failed to resume for ")) {
						new File("LOCAL + dirName + \"/\" + name").deleteOnExit();
						try {
							sftp.get(PREFIX + dirName + "/" + name, LOCAL + dirName + "/" + name, listen, ChannelSftp.RESUME);
							log(listen.getTotal(), listen.getSkip(), listen.getSum(), name);
						} catch (SftpException e1) {
							logger.error("", e1);
						}
					}
					logger.error("", e);
				}
				if (count.decrementAndGet() == 0) {
					done();
					try {
						sftp.put(LOCAL + dirName + "/download.log", PREFIX + dirName + "/download.log", listen,
								ChannelSftp.APPEND);
						log(listen.getTotal(), listen.getSkip(), listen.getSum(), "download.log");
					} catch (SftpException e) {
						logger.error("", e);
					}

					new File(LOCAL + dirName + "/download.log").deleteOnExit();
				}

				syncChannel(sftp);
				syncListen(listen);
			}

			if (wg.decrementAndGet() == 0) {
				synchronized (wg) {
					wg.notifyAll();
				}
			}
			semaphore.release();
		}
	}

	static class DeleteTask implements Runnable {
		private static List<String> files = new LinkedList<>();

		public static synchronized void addFile(String name) {
			files.add(name);
			wg.incrementAndGet();
		}

		private synchronized String getName() {
			return files.remove(0);
		}

		@Override
		public void run() {
			if (files.size() > 0 && CHANNELS.size() > 0) {
				String name = getName();
				ChannelSftp sftp = syncChannel(null);
				try {
					sftp.rm(name);
				} catch (SftpException e) {
					logger.error("", e);
				}
				syncChannel(sftp);
			}
			semaphore.release();
			wg.decrementAndGet();
		}
	}

	private static void initDate() {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1);
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
				System.out.println("remote dir isn't a directory");
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
			System.out.println(e.getMessage());
		}
		if (sftp != null) {
			syncChannel(sftp);
		}
	}

	private static boolean checkTime(String name) {
		Matcher matcher = pattern.matcher(name);
		if (matcher.matches()) {
			if (matcher.replaceAll("$1").compareTo(deadline) == -1) {
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

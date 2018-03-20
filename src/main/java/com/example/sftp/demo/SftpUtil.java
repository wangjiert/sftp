package com.example.sftp.demo;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

public class SftpUtil {
	private static Logger logger = LogManager.getLogger(SftpUtil.class);
	private static final int SERVERALIVEINTERVAL = 60000;

	public static boolean isHostReach(String host, Integer timeOut) {
		boolean result = true;
		try {
			result = InetAddress.getByName(host).isReachable(timeOut);
		} catch (UnknownHostException e) {
			logger.error("", e);
			System.out.printf("time:%s, thread :%s, Unable to resolve domain name\n", new Date().toString(), Thread.currentThread().getName());
			result = false;
		} catch (IOException e) {
			logger.error("", e);
			System.out.printf("time:%s, thread :%s, Network connection is abnormal\n", new Date().toString(), Thread.currentThread().getName());
			result = false;
		}
		return result;
	}

	public static boolean isHostConnect(String host, int port) {
		boolean result = false;
		Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(host, port));
			result = true;
		} catch (IOException e) {
			logger.error("", e);
			System.out.printf("time:%s, thread :%s, please check the server,ensure sftp service is normal\n", new Date().toString(), Thread.currentThread().getName());
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				logger.error("", e);
				// todo
			}
		}
		return result;
	}

	public boolean isLoginCorrect(String sftpHost, Integer sftpPort, String userName, String password,
			String privateKey, String passphrase, int timeout) throws Exception {
		boolean isLoginCorrect = false;
		JSch jsch = new JSch();

		if (userName == null || userName.equals("")) {
			return isLoginCorrect;
		}

		try {
			if (privateKey != null && !"".equals(privateKey)) {
				if (passphrase != null && "".equals(passphrase)) {
					jsch.addIdentity(privateKey, passphrase);
				} else {
					jsch.addIdentity(privateKey);
				}
			}

			Session session = jsch.getSession(userName, sftpHost, sftpPort);
			if (password != null && !"".equals(password)) {
				session.setPassword(password);
			}

			Properties sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");
			session.setConfig(sshConfig);
			session.setTimeout(timeout);
			session.setServerAliveInterval(60000);
			session.connect();
			isLoginCorrect = true;
			session.disconnect();

			System.out.println("--[] SFTP连接测试成功！host=" + sftpHost + ",user=" + userName);
		} catch (JSchException e) {
			System.out.println("--[] SFTP连接测试失败！host=" + sftpHost + ",user=" + userName);
			e.printStackTrace();
			isLoginCorrect = false;
		}

		return isLoginCorrect;
	}

	public boolean hasRWAuth(String host, int port, String userName, String password, String domain, String dir,
			int timeout) throws Exception {
		boolean result = false;
		ChannelSftp sftp = null;
		JSch jsch = new JSch();
		if (userName == null || userName.equals("")) {
			return result;
		}
		if (password == null || password.equals("")) {
			return result;
		}
		try {
			Session session = jsch.getSession(userName, host, port);
			if (password != null && !"".equals(password)) {
				session.setPassword(password);
			}
			session.setConfig("StrictHostKeyChecking", "no");
			session.setTimeout(timeout);
			session.setServerAliveInterval(60000);
			session.connect();
			Channel channel = session.openChannel("sftp");
			channel.connect();
			sftp = (ChannelSftp) channel;
			try {
				String uuid = UUID.randomUUID().toString();
				sftp.cd(dir);
				sftp.mkdir(uuid);
				sftp.rmdir(uuid);
				result = true;
			} catch (SftpException sException) {
				if (ChannelSftp.SSH_FX_PERMISSION_DENIED == sException.id) {
					System.err.println("SSH_FX_NO_SUCH_FILE ");
				}
				if (ChannelSftp.SSH_FX_NO_SUCH_FILE == sException.id) {
					System.err.println("SSH_FX_PERMISSION_DENIED ");
				}
				result = false;
			}
			channel.disconnect();
			session.disconnect();
		} catch (JSchException e) {
			e.printStackTrace();
			result = false;
		}
		return result;
	}

	public static ChannelSftp sftpConnect(String sftpHost, int sftpPort, String sftpUserName, String sftpPassword,
			String privateKey, String passphrase, int timeout) {
		JSch jsch = new JSch();
		Channel channel = null;
		try {
			if (privateKey != null && !"".equals(privateKey)) {
				// 使用密钥验证方式，密钥可以使有口令的密钥，也可以是没有口令的密钥
				if (passphrase != null && "".equals(passphrase)) {
					jsch.addIdentity(privateKey, passphrase);
				} else {
					jsch.addIdentity(privateKey);
				}
			}

			Session session = jsch.getSession(sftpUserName, sftpHost, sftpPort);
			if (sftpPassword != null && !"".equals(sftpPassword)) {
				session.setPassword(sftpPassword);
			}
			Properties sshConfig = new Properties();
			// do not verify host key
			sshConfig.put("StrictHostKeyChecking", "no");
			session.setConfig(sshConfig);
			session.setTimeout(timeout);
			session.setServerAliveInterval(SERVERALIVEINTERVAL);
			session.connect();

			// 参数sftp指明要打开的连接是sftp连接
			channel = session.openChannel("sftp");
			channel.connect();
		} catch (JSchException e) {
			logger.error("", e);
			System.out.printf("time:%s, thread :%s, can't connect to sftp server, username and password is wrong or network is busy\n", new Date().toString(), Thread.currentThread().getName());
		}

		return (ChannelSftp) channel;
	}

	public static void uploadFile(ChannelSftp sftp, SftpProgressMonitorImpl listen, String remoteFilePath, File file,
			int type) {
		try {
			if (type == ChannelSftp.RESUME) {
				try {
					SftpATTRS stat = sftp.stat(file.getName());
					if (stat.getMTime() < file.lastModified() / 1000) {
						sftp.rm(file.getName());
					}
				} catch (SftpException e) {
					if (!e.getMessage().contains("No such file")) {
						logger.error("", e);
						return;
					}
				}
			}
			listen.reset();
			sftp.put(file.getAbsolutePath(), file.getName(), listen, type);
			if (listen.getSum() != 0) {
				new FileRecord(file.getName(), new Date(), listen.getTotal(), listen.getSkip(), listen.getSum()).log();
			}
			synchronized (SftpUpload.endSignal) {
				if (listen.getSum() == 0) {
					System.out.println("skip file " + file.getName());
				} else {
					if (listen.getSkip() > 0) {
						System.out.printf("file name: %s, file length: %d, skip length: %d, read length: %d\n",
								file.getName(), listen.getTotal(), listen.getSkip(), listen.getSum());
					} else {
						System.out.printf("file name: %s, file length: %d, read length: %d\n", file.getName(),
								listen.getTotal(), listen.getSum());
					}
				}
			}
		} catch (SftpException e) {
			if (e.getMessage().equals("failed to resume for " + remoteFilePath + File.separator + file.getName())) {
				try {
					sftp.rm(file.getName());
					uploadFile(sftp, listen, remoteFilePath, file, type);
				} catch (SftpException e1) {
					logger.error("", e);
				}
			}
		}
	}

	public static void makeDir(ChannelSftp sftp, String dir) throws SftpException {
		sftp.cd("/");
		dir = dir.replaceAll("//", "/");
		String[] folders = dir.split("/");
		for (String folder : folders) {
			if (folder.length() > 0) {
				try {
					sftp.cd(folder);
				} catch (SftpException e) {
					sftp.mkdir(folder);
					sftp.cd(folder);
				}
			}
		}
	}

	public static boolean isDirExist(ChannelSftp sftp, String dir) {
		boolean result = false;
		try {
			sftp.cd(dir);
			result = true;
		} catch (SftpException e) {
			result = false;
		} catch (Exception e) {
			result = false;
		}

		return result;
	}

	public static void disconnected(ChannelSftp sftp) {
		if (sftp != null) {
			try {
				sftp.getSession().disconnect();
			} catch (JSchException e) {
				e.printStackTrace();
			}
			sftp.disconnect();
		}
	}

	static class SftpProgressMonitorImpl implements SftpProgressMonitor {
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

		public boolean count(long count) {
			if (!append && init && count > 0) {
				skip = count;
				count = 0;
			}
			init = false;
			sum += count;
			return true;
		}

		public void end() {
		}

		public void init(int op, String src, String dest, long max) {
			total = max;
			if (src.contains("finish.log")) {
				append= true;
			}
		}
	}
}

class FileRecord {
	private static Logger logger = LogManager.getLogger("com.example.sftp.demo.FileRecord");
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String name;
	private long size;
	private long skip;
	private long upload;
	private Date date;

	public FileRecord(String name, Date date, Long size, long skip, long upload) {
		this.name = name;
		this.date = date;
		this.size = size;
		this.skip = skip;
		this.upload = upload;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public long getSkip() {
		return skip;
	}

	public void setSkip(long skip) {
		this.skip = skip;
	}

	public long getUpload() {
		return upload;
	}

	public void setUpload(long upload) {
		this.upload = upload;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String toString() {
		return name + "\t" + format.format(date) + "\t" + size + "\t" + skip + "\t" + upload;
	}

	public void log() {
		logger.info(String.valueOf(this));
	}
}

package com.example.sftp.demo;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DBUtil {
    private static Logger logger = LogManager.getLogger(DBUtil.class);
    private static  String driver;
    private static  String url;
    private static  String user;
    private static  String password;
    private static final char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static Connection con;
    private static PreparedStatement insert;
    private static PreparedStatement query;
    private static PreparedStatement update;

    private static boolean parseConfig() {
        File conf = new File(System.getProperty("SFTP_HOME") + "../conf/my.conf");
        if (conf.exists()) {
            Properties prop = parseConfig(conf.getAbsolutePath());
            if (prop != null) {
               if (prop.getProperty("db.driver").equals("mysql")) {
                   driver = "com.mysql.cj.jdbc.Driver";
                   url = "jdbc:mysql://"+ prop.getProperty("db.host") + "/" + prop.getProperty("db.dbname") + "?useSSL=false";
                   user = prop.getProperty("db.user");
                   password = prop.getProperty("db.password");
                   return true;
               }
            }
        }
        return false;
    }

    private static Properties parseConfig(String filename) {
        Pattern pattern = Pattern.compile("^\\[.*\\]$");
        try (BufferedReader f = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));) {
            while (true) {
                String line = f.readLine();
                if (line == null) {
                    return null;
                }
                line = line.trim();
                if (line.equals("[database]")) {
                    break;
                }
            }
            Properties properties = new Properties();
            while (true) {
                String line = f.readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.startsWith("#")) {
                    continue;
                } else if (line.equals("")) {
                    continue;
                } else if (pattern.matcher(line).matches()) {
                    break;
                } else {
                    String[] lines = line.split("=");
                    if (lines.length != 2) {
                        continue;
                    }
                    properties.setProperty(lines[0].trim(), lines[1].trim());
                }
            }
            // fileServerInfo = new FileServerInfo(properties);
            return properties;
        } catch (FileNotFoundException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }
        return null;
    }

    public static boolean init() {
        boolean config = parseConfig();
        if (!config) {
            return false;
        }
        try {
            //加载驱动程序
            Class.forName(driver);
            //1.getConnection()方法，连接MySQL数据库！！
            con = DriverManager.getConnection(url, user, password);
            //2.创建statement类对象，用来执行SQL语句！！
            insert = con.prepareStatement("insert into sftp_record values(?,?,?,?,0,0)");
            query = con.prepareStatement("select count(*) from sftp_record where id=?");
            update = con.prepareStatement("update sftp_record set modify_time=?,size=?,is_done=0,is_converted=0 where id=?");
            //要执行的SQL语句
            return true;
        } catch (ClassNotFoundException e) {
            //数据库驱动类异常处理
            logger.error("", e);
        } catch (SQLException e) {
            //数据库连接失败异常处理
            logger.error("", e);
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("", e);
        }
        return false;
    }

    public static void close() {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                logger.error("", e);
            }
        }
    }

    public static synchronized boolean addRow(String name, long size, long time) {
        ResultSet rs = null;
        try {
            query.setString(1, getMd5(name));
            rs = query.executeQuery();
            rs.next();
            if (rs.getInt(1) > 0) {
                update.setLong(1, time);
                update.setLong(2, size);
                update.setString(3, getMd5(name));
                return (update.executeUpdate() > 0);
            } else {
                insert.setString(1, getMd5(name));
                insert.setString(2, name);
                insert.setLong(3,size);
                insert.setLong(4, time);
                return (insert.executeUpdate() > 0);
            }
        } catch (SQLException e) {
            logger.error("", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("", e);
                }
            }
        }
        return false;
    }

    private static String getMd5(String value) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] inputByteArray = value.getBytes();
            messageDigest.update(inputByteArray);
            byte[] resultByteArray = messageDigest.digest();
            return byteArrayToHex(resultByteArray);
        } catch (NoSuchAlgorithmException e) {
            logger.error("", e);
        }
        return "";
    }

    private static String byteArrayToHex(byte[] byteArray) {
        int index = 0;
        char[] resultCharArray = new char[byteArray.length * 2];
        for (byte b : byteArray) {
            resultCharArray[index++] = hexDigits[b >>> 4 & 0xf];
            resultCharArray[index++] = hexDigits[b & 0xf];
        }
        return new String(resultCharArray);
    }

    public static void main(String[] args) {
        System.out.println(getMd5("sas"));
    }
}

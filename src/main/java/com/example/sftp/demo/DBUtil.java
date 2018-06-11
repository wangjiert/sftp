package com.example.sftp.demo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DBUtil {
    private static Logger logger = LogManager.getLogger(DBUtil.class);
    private static final char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static String driver;
    private static String url;
    private static String user;
    private static String password;
    private static Connection con;
    private static PreparedStatement columns;
    private static Statement convert;
    private static PreparedStatement get;
    private static PreparedStatement add;
    private static PreparedStatement update;

    //读取数据库连接配置
    private static boolean parseConfig() {
        Path conf = Paths.get(System.getProperty("SFTP_HOME"), "../conf/my.conf");
        if (Files.exists(conf)) {
            Properties prop = ParseConfig.parse(conf, "localdb", null);
            if (prop != null) {
                if (prop.getProperty("db.driver").equals("mysql")) {
                    driver = "com.mysql.cj.jdbc.Driver";
                    url = "jdbc:mysql://" + prop.getProperty("db.host") + "/" + prop.getProperty("db.dbname") + "?useSSL=false";
                    user = prop.getProperty("db.user");
                    password = prop.getProperty("db.password");
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean init() {
        boolean config = parseConfig();
        if (!config) {
            return false;
        }
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
            convert = con.createStatement();
            columns = con.prepareStatement("select column_name from information_schema.columns where table_schema='dumpdb' and table_name=?");
            add = con.prepareStatement("insert into sftp_record values(?,?,?)");
            add.execute("create table if not exists sftp_record(id varchar(255) not null primary key , name varchar(255) not null, update_time bigint(20) not null )");
            get = con.prepareStatement("select update_time from sftp_record where id=?");
            update = con.prepareStatement("update sftp_record set update_time=? where id=?");
            return true;
        } catch (ClassNotFoundException e) {
            logger.error("", e);
        } catch (SQLException e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        }
        return false;
    }

    static void addRecord(String name, int modifyTime) {
        ResultSet rs = null;
        try {
            get.setString(1, getMd5(name));
            rs = get.executeQuery();
            if (rs.next()) {
                update.setInt(1, modifyTime);
                update.setString(2, getMd5(name));
                update.executeUpdate();
            } else {
                add.setString(1, getMd5(name));
                add.setString(2, name);
                add.setInt(3, modifyTime);
                add.executeUpdate();
            }
        } catch (SQLException e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            closeResource(rs);
        }
    }

    static boolean isExist(String name) {
        ResultSet rs = null;
        try {
            get.setString(1, getMd5(name));
            rs = get.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            closeResource(rs);
        }
        return false;
    }

    static int get(String name) {
        ResultSet rs = null;
        try {
            get.setString(1, getMd5(name));
            rs = get.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return 0;
            }
        } catch (SQLException e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            closeResource(rs);
        }
        return 0;
    }

    public static void close() {
        closeResource(con);
    }

    //把.MYI .MYD .frm三个文件转成.mysql
    static boolean convert(String dbTable, String outName) {
        //如果是索引有问题就修复一下
        boolean goon = false;

        do {
            try {
                if (goon) {
                    ConvertCheck.emptyDir("/var/lib/mysql-files");
                    convert.execute("repair table " + dbTable);
                }
                convert.execute("flush table " + dbTable);
                convert.execute("select * from " + dbTable + " into outfile '" + outName + "' fields terminated by '|'");
                String[] dbTables = dbTable.split("\\.");

                return true;
            } catch (SQLException e) {
                if (e.getMessage().contains("is marked as crashed and should be repaired")) {
                    goon = !goon;
                } else {
                    goon = false;
                    logger.error("", e);
                }
            } catch (Throwable e) {
                logger.error("", e);
            }
        } while (goon);

        return false;
    }

    //获取数据表的列名
    static String getHead(String table) {
        ResultSet rs = null;
        try {
            columns.setString(1, table);
            rs = columns.executeQuery();
            Set<String> heads = new HashSet<>();
            while (rs.next()) {
                heads.add(rs.getString(1));
            }
            if (heads.size() > 0) {
                return StringUtils.join(heads, '|');
            }
        } catch (SQLException e) {
            logger.error("", e);
        } catch (Throwable e) {
            logger.error("", e);
        } finally {
            closeResource(rs);
        }
        return null;
    }

    private static void closeResource(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
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
}
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
    private static PreparedStatement columns;
    private static Statement convert;

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
            insert = con.prepareStatement("insert into sftp_record values(?,?,?,?,?,?,0,0,?)");
            query = con.prepareStatement("select count(*) from sftp_record where id=?");
            update = con.prepareStatement("update sftp_record set update_time=?,modify_time=?,size=?,is_done=0,is_found=0 where id=?");
            convert = con.createStatement();
            columns = con.prepareStatement("select column_name from information_schema.columns where table_schema='dumpdb' and table_name=?");
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
                update.setTimestamp(1, new Timestamp(new java.util.Date().getTime()));
                update.setLong(2, time);
                update.setLong(3, size);
                update.setString(4, getMd5(name));
                return (update.executeUpdate() > 0);
            } else {
                insert.setString(1, getMd5(name));
                insert.setString(2, name);
                String dirName = name.substring(0, name.lastIndexOf("/"));
                if (name.endsWith(".csv")) {
                    insert.setString(3, name.substring(0, name.indexOf("e_cdr") - 1));
                } else {
                    insert.setString(3, dirName.substring(dirName.lastIndexOf("/")+1));
                }
                insert.setString(4, dirName);
                insert.setLong(5,size);
                insert.setLong(6, time);
                insert.setTimestamp(7, new Timestamp(new java.util.Date().getTime()));
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

    static boolean convert(String dbTable, String outName) {
        try {
            convert.execute("flush table "+dbTable);
            convert.execute("select * from "+dbTable+" into outfile '"+outName+"' fields terminated by '|'");
            return true;
        } catch (SQLException e) {
            try {
                if (e.getMessage().contains("is marked as crashed and should be repaired")) {
                    ConvertCheck.emptyDir("/var/lib/mysql-files");
                    convert.execute("repair table "+dbTable);
                    convert.execute("flush table "+dbTable);
                    convert.execute("select * from "+dbTable+" into outfile '"+outName+"' fields terminated by '|'");
                    return true;
                } else {
                    logger.error("", e);
                }
            } catch (SQLException e1) {
                logger.error("", e1);
            }
        }
        return false;
    }

    static String getHead(String table) {
        ResultSet rs = null;
        try {
            columns.setString(1, table);
            rs = columns.executeQuery();
            String heads = "";
            while (rs.next()) {
                heads += rs.getString(1)+"|";
            }
            if (heads.charAt(heads.length()-1) == '|') {
                heads = heads.substring(0, heads.length()-1);
            }
            if (!heads.equals("")) {
                return heads;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    public static void main(String[] args) {
        try {
            //加载驱动程序
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://192.168.130.201/gaxz?useSSL=false", "root", "123456.abcd");
            convert = con.createStatement();
            try {
                boolean ss = convert.execute("flush table dumpdb.e_customer20180315");
                convert.execute("select * from dumpdb.e_customer20180315 into outfile '/var/lib/mysql-files/e_customer20180315.sql' fields terminated by '|'");
                System.out.println(ss);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            //数据库驱动类异常处理
            e.printStackTrace();
        } catch (SQLException e) {
            //数据库连接失败异常处理
            e.printStackTrace();
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
}

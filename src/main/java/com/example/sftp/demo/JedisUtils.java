package com.example.sftp.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

public class JedisUtils {
    private static final Logger logger = LogManager.getLogger(JedisUtils.class);
    private static final LinkedHashMap<String, String> trans = new LinkedHashMap<>();
    private static String local;
    private static String dump;
    private static String ip;
    private static int port;
    private static String auth;
    private static Jedis jedis;
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

    private static Object toObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (Exception ex) {
            logger.error("parse result err:", ex);
        }
        return obj;
    }

    private static void inflatMap() {
        trans.put("Server", "vosip");
        trans.put("domain", "");
        trans.put("http_port", "");
        trans.put("camp_name", "vos_name");
        trans.put("Calling", "callere164");
        trans.put("called", "calleee164");
        trans.put("disposition", "pdd");
        trans.put("minute", "holdtime");
        trans.put("calldate", "starttime");
        trans.put("wav", "");
        trans.put("callerproductid", "");
    }

    private static void init() {
        inflatMap();
        jedis = new Jedis(ip, port);
        jedis.auth(auth);
        jedis.connect();
    }

    private static void destroy() {
        if (jedis != null) {
            jedis.disconnect();
            jedis.close();
        }
    }

    private static String createAndWrite(String name, Map<String, String> content) {
        File f = new File(name);
        FileWriter writer = null;
        try {
            writer = new FileWriter(f, true);
            List<String> line = new LinkedList<>();
            if (f.length() == 0) {
                for (String key : trans.keySet()) {
                    if (trans.get(key).equals(""))
                        line.add(key);
                    else
                        line.add(trans.get(key));
                }
                writer.write(StringUtils.join(line, "|"));
                writer.write("\n");
                line.clear();
            }
            for (String key : trans.keySet()) {
                if (key.equals("minute")) {
                    String[] ms = content.get("minute").split(":");
                    line.add(Integer.parseInt(ms[0]) + Integer.parseInt(ms[1]) + "");
                } else if (key.equals("callerproductid")) {
                    line.add("z99");
                } else
                    line.add(content.get(key));
            }
            writer.write(StringUtils.join(line, "|"));
            writer.write("\n");
            return name;
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    logger.error("close err:", e);
                }
            }
        }
        return null;
    }

    public static void doJedis() {
        while (true) {
            List<byte[]> result = jedis.brpop(2000, "qh_cdr".getBytes());
            //String key = new String(result.get(0));
            byte[] value = result.get(1);
            Object t = toObject(value);
            if (t != null) {
                List<String> allFiles = new LinkedList<>();
                ArrayList<Map<String, String>> CDR = (ArrayList<Map<String, String>>) t;
                if (CDR.size() == 0) {
                    break;
                }
                for (Map<String, String> map : CDR) {
                    new File(local + "/" + map.get("Server")).mkdirs();
                    String tmp = createAndWrite(local + "/" + map.get("Server") + "/" + "e_cdr_" + format.format(new Date()) + ".mysql", map);
                    if (tmp != null) {
                        allFiles.add(tmp);
                        System.out.printf("time:%s, thread :%s, create file: %s\n", new Date().toString(),
                                Thread.currentThread().getName(), tmp);
                    }
                }
                for (String tmp : allFiles) {
                    try {
                        Path dest = Paths.get(tmp.replace(local, dump));
                        Files.createDirectories(dest.getParent());
                        Files.copy(new File(tmp).toPath(), dest);
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                }
            } else {
                break;
            }
        }
    }

    public static void go(Path conf, String local, String dump) {
        Properties prop = ParseConfig.parse(conf, "redis", null);
        if (prop == null)
            return;

        JedisUtils.local = local + "/redis";
        JedisUtils.dump = dump + "/redis";
        JedisUtils.ip = prop.getProperty("ip");
        JedisUtils.port = Integer.parseInt(prop.getProperty("port"));
        JedisUtils.auth = prop.getProperty("auth");
        init();
        doJedis();
        destroy();
    }
}

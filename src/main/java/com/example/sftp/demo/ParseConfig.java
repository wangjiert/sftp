package com.example.sftp.demo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.regex.Pattern;

public class ParseConfig {
    private static Logger logger = LogManager.getLogger(ParseConfig.class);
    public static Properties parse(String filename, String parent, String field) {
        Pattern pattern = Pattern.compile("^\\[.*\\]$");
        try (BufferedReader f = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));) {
            while (true) {
                String line = f.readLine();
                if (line == null) {
                    return null;
                }
                line = line.trim();
                if (line.equals("["+parent+"]")) {
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
                if (pattern.matcher(line).matches()) {
                    break;
                } else if (line.startsWith("#") || line.equals("")) {
                    continue;
                }else {
                    String[] lines = line.split("=");
                    if (lines.length != 2) {
                        continue;
                    }
                    if (field != null && field.equals(lines[0].trim())) {
                        properties.setProperty(parent+"."+field, lines[1].trim());
                        break;
                    } else if (field == null) {
                        properties.setProperty(lines[0].trim(), lines[1].trim());
                    }
                }
            }
            return properties;
        } catch (FileNotFoundException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }
        return null;
    }
}

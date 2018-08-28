package com.example.sftp.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Date;
import java.util.zip.GZIPInputStream;

public class GzHandle {
    private static final Logger logger = LogManager.getLogger(GzHandle.class);
    private final static int BUFFER = 1048576;


    public static void doGz(String dirName, String path, String rawName) {
        rawName = StringUtils.removeEnd(rawName, ".gz");
        rawName = rawName.replaceAll(" ", "");

        String fileName = StringUtils.removeEnd(path, ".gz");
        if (unGzipFile(path)) {
            String outName = "/var/lib/mysql-files/" + rawName + ".mysql";
            if (DBUtil.doImportAndExport(fileName, outName)) {
                String dumpPath = ConvertCheck.getOutpath(dirName, rawName + ".mysql", true);
                String heads = "id|ext_number|ip|criminal_ip|port|information|hits|created_at|updated_at";
                Process p = null;
                try {
                    p = Runtime.getRuntime().exec(new String[]{"sh", "-c", "sed -i '1i\\" + heads + "' " + outName + " && mv " + outName + " " + dumpPath});
                    p.waitFor();
                    System.out.printf("time:%s, thread :%s, convert file %s successed!\n",
                            new Date().toString(), Thread.currentThread().getName(), outName);
                } catch (Exception e) {
                    logger.error("write file head and mv file err:", e);
                    new File(outName).deleteOnExit();
                    System.out.printf("time:%s, thread :%s, write and mv file %s failed\n",
                            new Date().toString(), Thread.currentThread().getName(), outName);
                }
            } else {
                System.out.printf("time:%s, thread :%s, convert file %s failed\n",
                        new Date().toString(), Thread.currentThread().getName(), outName);
            }
            new File(fileName).deleteOnExit();
        }
    }

    public static boolean unGzipFile(String gz) {
        FileInputStream fin = null;
        GZIPInputStream gzin = null;
        FileOutputStream fout = null;
        try {
            fin = new FileInputStream(gz);
            gzin = new GZIPInputStream(fin);
            fout = new FileOutputStream(StringUtils.removeEnd(gz, ".gz"));

            int num;
            byte[] buf = new byte[4096];

            while ((num = gzin.read(buf, 0, buf.length)) != -1) {
                fout.write(buf, 0, num);
            }
            return true;
        } catch (Exception e) {
            logger.error("handle gz file err:", e);
        } finally {
            try {
                if (gzin != null) gzin.close();
                if (fout != null) fout.close();
                if (fin != null) fin.close();
            } catch (Exception e) {
                logger.error("close file err:", e);
            }
        }
        return false;
    }
}

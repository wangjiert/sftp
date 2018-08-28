package com.example.sftp.demo;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
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

    private static Set<String> decompress(String tar_gz, String sourceFolder) {
        HashSet<String> files = new HashSet<>();
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        GZIPInputStream gzis = null;
        TarArchiveInputStream tais = null;
        OutputStream out = null;
        try {
            fis = new FileInputStream(tar_gz);
            bis = new BufferedInputStream(fis);
            gzis = new GZIPInputStream(bis);
            tais = new TarArchiveInputStream(gzis);
            TarArchiveEntry tae = null;
            boolean flag = false;

            while ((tae = tais.getNextTarEntry()) != null) {
                File tmpFile = new File(sourceFolder + tae.getName());
                if (!flag) {
                    new File(tmpFile.getParent()).mkdirs();
                    flag = true;
                }
                out = new FileOutputStream(tmpFile);
                int length = 0;
                byte[] b = new byte[BUFFER];
                while ((length = tais.read(b)) != -1) {
                    out.write(b, 0, length);
                }
                files.add(tmpFile.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.error("handle tz file err:", e);
        } finally {
            try {
                if (tais != null) tais.close();
                if (gzis != null) gzis.close();
                if (bis != null) bis.close();
                if (fis != null) fis.close();
                if (out != null) {
                    out.flush();
                    out.close();
                }
            } catch (Exception e) {
                logger.error("close file err:", e);
            }
        }
        return files;
    }

}

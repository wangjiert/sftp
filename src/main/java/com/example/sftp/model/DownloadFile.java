package com.example.sftp.model;

import java.util.Date;

public class DownloadFile {
    private String name;
    private Date midifierTime;
    private long size;
    private boolean isHandleed;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getMidifierTime() {
        return midifierTime;
    }

    public void setMidifierTime(Date midifierTime) {
        this.midifierTime = midifierTime;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isHandleed() {
        return isHandleed;
    }

    public void setHandleed(boolean handleed) {
        isHandleed = handleed;
    }
}

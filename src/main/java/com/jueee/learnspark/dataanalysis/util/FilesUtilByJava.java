package com.jueee.learnspark.dataanalysis.util;

import java.io.File;

public class FilesUtilByJava {

    /**
     * 获取项目跟路径
     * @return
     */
    public static String getRootPath(){
        return new File("").getAbsolutePath();
    }

    /**
     * 获取配置文件路径
     * @return
     */
    public static String getResourcePath(){
        return getRootPath() + File.separator + "src" + File.separator +"main" + File.separator + "resources";
    }

    /**
     * 获取数据文件路径
     * @return
     */
    public static String getDataPath(){
        return getResourcePath() + File.separator + "data";
    }

    public static void main(String[] args) {
        System.out.println(getRootPath());
        System.out.println(getResourcePath());
        System.out.println(getDataPath());
    }
}

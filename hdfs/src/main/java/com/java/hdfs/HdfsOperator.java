package com.java.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created by sgr on 2017/10/14/014.
 */
public class HdfsOperator {
    FileSystem fileSystem;
    public boolean init(String uri) throws IOException {
        //加载src目录下的配置文件
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(uri),configuration);
        return fileSystem != null;
    }

    public void destroy(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean mkdir(String dir) throws IOException {
        Path path = new Path(URI.create(dir));
        return fileSystem.mkdirs(path);
    }

    public boolean deleteDir(String dir,boolean recursive){
        Path path = new Path(URI.create(dir));
        try {
            return fileSystem.delete(path,recursive);
        } catch (IOException e) {
            return false;
        }
    }

    public boolean createFile(String fileName) throws IOException {
        Path path = new Path(URI.create(fileName));
        return fileSystem.createNewFile(path);
    }

    public boolean uploadFile(String srcFile, String destPath){
        try {
            File file = new File(srcFile);
            FSDataOutputStream outputStream = fileSystem.create(new Path(StringUtils.join(destPath,"/",file.getName())));
            FileUtils.copyFile(file,outputStream);
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}

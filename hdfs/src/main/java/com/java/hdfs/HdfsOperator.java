package com.java.hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * 删除目录或文件
     * @param dir 目录或文件地址
     * @param recursive 是否递归删除
     * @return boolean
     */
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

    /**
     * 将多个小文件合并上传（k：文件名 v:文件内容 kv方式合并）
     * @param path 源文件的目录
     * @param destPath 目标目录
     * @return boolean
     */
    public boolean uploadFiles(String path,String destPath) {
        try {
            File files = new File(path);
            Path name = new Path(StringUtils.join(destPath,"/",files.getName(),".seq"));
            SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem,new Configuration(), name, Text.class,Text.class);
            for (File file : files.listFiles()){
                writer.append(new Text(file.getName()),new Text(FileUtils.readFileToString(file)));
            }
        }catch (IOException e){
            return false;
        }
        return true;
    }

    /**
     * 将大文件还原下载成多个小文件
     * @param srcFile hdfs源文件
     * @param destPath 目标目录
     * @return boolean
     */
    public boolean downloadSequenceFile(String srcFile,String destPath){
        Path file = new Path(srcFile);
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem,file,new Configuration());
            Text key = new Text();
            Text value = new Text();
            while (reader.next(key,value)){
                File f = new File(StringUtils.join(destPath,"/",key.toString()));
                FileUtils.writeStringToFile(f,value.toString());
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public List<String> listFiles(String dir,boolean recursive) throws IOException {
        Path path = new Path(dir);
        List<String> files = new ArrayList<String>();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path,recursive);
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            files.add(fileStatus.getPath().getName());
        }
        return files;
    }
}

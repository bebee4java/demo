import com.java.hdfs.HdfsOperator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by sgr on 2017/10/14/014.
 */
public class TestUnit {
    HdfsOperator hdfsOperator;
    @Before
    public void before() throws IOException {
        hdfsOperator = new HdfsOperator();
        hdfsOperator.init("hdfs://192.168.16.197:9000");
    }
    @After
    public void after(){
        hdfsOperator.destroy();
    }

    @Test
    public void testMkdir() throws IOException {
        if (hdfsOperator.mkdir("/ss")){
            System.out.println("创建成功");
        }
    }
    @Test
    public void testDeleteDir(){
        if (hdfsOperator.deleteDir("/sgr/test",false)){
            System.out.println("删除成功");
        }
    }
    @Test
    public void testUploadFile(){
        if (hdfsOperator.uploadFile("E:\\temp\\test.txt","/sgr")){
            System.out.println("上传成功");
        }
    }

    @Test
    public void testUploadFiles(){
        if (hdfsOperator.uploadFiles("E:\\temp\\test","/sgr")){
            System.out.println("上传成功");
        }
    }

    @Test
    public void testDownloadSequenceFile(){
        if (hdfsOperator.downloadSequenceFile("/sgr/test.seq","E:\\temp")){
            System.out.println("下载成功");
        }
    }

    @Test
    public void testListFiles() throws IOException {
        List<String> files = hdfsOperator.listFiles("/sgr",false);
        System.out.println(files);
    }
}

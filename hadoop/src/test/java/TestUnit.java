import com.java.hadoop.hdfs.HdfsOperator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sgr on 2017/10/14/014.
 */
public class TestUnit {
    HdfsOperator hdfsOperator;
    @Before
    public void before() throws IOException {
        hdfsOperator = new HdfsOperator();
        /*hdfsOperator.setHa(false);
        hdfsOperator.setFs_defaultFS("hdfs://192.168.16.197:9000");*/

        hdfsOperator.setHa(true);
        hdfsOperator.setFs_defaultFS("hdfs://nameservices");
        hdfsOperator.setDfs_ha_namenodes("namenode232,namenode199");
        hdfsOperator.setDfs_nameservices("nameservices");
        Map<String,String> map = new HashMap<String, String>();
        map.put("namenode232","node1:8020");
        map.put("namenode199","node2:8020");
        hdfsOperator.setDfs_namenode_rpc_address(map);
        hdfsOperator.init();
    }
    @After
    public void after(){
        hdfsOperator.destroy();
    }

    @Test
    public void testMkdir() throws IOException {
        if (hdfsOperator.mkdir("/sgr/wc/input")){
            System.out.println("创建成功");
        }
    }
    @Test
    public void testDeleteDir(){
        if (hdfsOperator.deleteDir("/ss",false)){
            System.out.println("删除成功");
        }
    }
    @Test
    public void testUploadFile(){
        if (hdfsOperator.uploadFile("E:\\Project\\demo\\hadoop\\data\\wc","/sgr/wc/input")){
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

    @Test
    public void testReadFile() throws IOException {
        System.out.println(hdfsOperator.readFile("/sgr/test.seq"));
    }
}

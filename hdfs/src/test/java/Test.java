import com.java.hdfs.HdfsOperator;

import java.io.IOException;

/**
 * Created by sgr on 2017/10/14/014.
 */
public class Test {
    public static void main(String[] args) {
        HdfsOperator hdfsOperator = new HdfsOperator();
        try {
            hdfsOperator.init("hdfs://192.168.16.197:9000");
            /*if (hdfsOperator.mkdir("/ss")){
                System.out.println("创建成功");
            }*/
//            hdfsOperator.deleteDir("/ss",true);
            boolean b = hdfsOperator.uploadFile("E:\\temp\\test.txt","/sgr");
            System.out.println(b);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            hdfsOperator.destroy();
        }
    }
}

import com.java.hadoop.hbase.HbaseClient;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Created by sgr on 2017/11/4.
 */
public class HbaseClientTest {
    public static void main(String[] args) {
        HbaseClient hbaseClient = new HbaseClient();
        if (hbaseClient.init()){
            System.out.println("Hbase client init succeed!");
            HTable hTable = hbaseClient.createTable("ticket","cf1");
            if (hTable != null){
                System.out.println("ticket table create succeed!");
            }else {
                System.out.println("ticket table create failed!");
            }
            hbaseClient.closeTable(hTable);
            hbaseClient.close();
        }else {
            System.out.println("Hbase client init failed!");
        }
    }
}

import com.java.hadoop.hbase.HbaseClient;
import com.java.hadoop.hbase.MyCell;
import com.java.hadoop.hbase.Ticket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by sgr on 2017/11/4.
 */
public class HbaseClientTest {
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");
    HbaseClient hbaseClient;

    @Before
    public void before(){
        hbaseClient = new HbaseClient("ticket1");
        boolean init = hbaseClient.init();
        if (init){
            System.out.println("hbaseClient init succeed!");
        }else {
            System.out.println("hbaseClient init failed!");
        }
    }

    @Test
    public void createTable(){
        boolean b = hbaseClient.createTable(hbaseClient.getTableName(),"cf1");
        if (b){
            System.out.println("创建表成功");
        }else {
            System.out.println("创建表失败");
        }
    }

    @Test
    public void insertTable() throws IOException {
        String number = randomNumber("183");
        String time = randomDate(2017);
        String rowKey = number + "_" + time;
        String cf = "cf1";
        Map<String,Object> columnMap = new HashMap<String, Object>();
        columnMap.put("type", "0");
        columnMap.put("dialNumber", randomNumber("182"));
        columnMap.put("time", time);
        hbaseClient.insert(rowKey,cf,columnMap);

    }

    @Test
    /*
     * 批量插入10个手机号100条通话记录
     * 按时间降序排序
     */
    public void insertBatchTable() throws ParseException, IOException {
        List<MyCell> cellList = new ArrayList<MyCell>();
        for (int i=0; i<10; i++){
            String rowKey;
            String number = randomNumber("186");
            for (int j=0; j<100; j++){
                String timeStr = randomDate(2017);
                Date date = DATE_FORMAT.parse(timeStr);
                long t = Long.MAX_VALUE - date.getTime();//按时间降序排序
                rowKey = number + "_" + t;
                MyCell type = new MyCell(rowKey,"cf1","type",new Random().nextInt(2)+"");
                MyCell time = new MyCell(rowKey,"cf1","time",timeStr);
                MyCell dialNumber = new MyCell(rowKey,"cf1","dialNumber",randomNumber("181"));
                cellList.add(type);
                cellList.add(time);
                cellList.add(dialNumber);
                System.out.println(rowKey);
            }
        }
        hbaseClient.insertBatch(cellList);
    }

    /**
     * 使用protobuf将数据序列化后存入Hbase,以减少存储
     * @throws ParseException
     * @throws IOException
     */
    @Test
    public void porotobufIn2Table() throws ParseException, IOException {
        for (int i=0; i<10; i++){
            String number = randomNumber("186");
            String timeStr = randomDate(2017,10,1);
            Date date = DATE_FORMAT.parse(timeStr);
            long t = Long.MAX_VALUE - date.getTime();//按时间降序排序
            String rowKey = number + "_" + t;

            Ticket.dticket.Builder dticket = Ticket.dticket.newBuilder();

            for (int j=0; j<100; j++){
                Ticket.ticket.Builder ticket = Ticket.ticket.newBuilder();
                ticket.setDialNumber(randomNumber("181"));
                ticket.setTime(timeStr);
                ticket.setType(new Random().nextInt(2)+"");

                dticket.addTicklist(ticket);
            }
            Map<String,Object> map = new HashMap<String,Object>();
            map.put("dticket",dticket.build());
            hbaseClient.insert(rowKey,"cf1",map);


        }
    }
    @Test
    public void getTable() throws IOException {
        String rowKey = "18373300853_20170826192139";
        String cf = "cf1";
        List<MyCell> cellList = hbaseClient.get(rowKey,cf,"dialNumber","type","time");
        for (MyCell myCell : cellList)
            System.out.println(myCell);
    }

    @Test
    public void getTableProtobufData() throws IOException {
        String rowKey = "1861631484_9223370527365891807";
        String cf = "cf1";
        List<MyCell> cellList = hbaseClient.get(rowKey,cf,"dticket");
        for (MyCell myCell : cellList){
            String rk = myCell.getRowKey();
            Object o = myCell.getCloumnValue();
            if (o instanceof Ticket.dticket){
                Ticket.dticket dticket = (Ticket.dticket) o;
                List<Ticket.ticket> tickets = dticket.getTicklistList();
                for (Ticket.ticket ticket : tickets){
                    String s = ticket.getType() + "|" + ticket.getDialNumber() + "|" + ticket.getTime();
                    System.out.println(s);
                }

            }
        }
    }

    @Test
    /**
     * 查询某个手机号近一个月的通话详单
     */
    public void ScanTable() throws ParseException, IOException {
        String number = "18692220880";
        //5月
        String startRow = number + "_" + (Long.MAX_VALUE - DATE_FORMAT.parse("20170601000000").getTime());
        String stopRow = number + "_" + (Long.MAX_VALUE - DATE_FORMAT.parse("20170501000000").getTime());
        String cf = "cf1";
        List<MyCell> cellList = hbaseClient.scanTable(startRow,stopRow,cf,"type","time","dialNumber");
        for (MyCell myCell : cellList){
            System.out.println(myCell);
        }
    }
    @Test
    /**
     * 查询某个号码所有主叫的通话详单（type=0）
     */
    public void ScanTableWithFilter() throws IOException {
        List<MyCell> cellList = null;
        cellList = hbaseClient.scanTableWithFilter(
                hbaseClient.getFilterList(true,"cf1","rowKey:18692220880","type=0"),
                "cf1",
                "type","time","dialNumber"
        );
        for (MyCell myCell : cellList){
            System.out.println(myCell);
        }

    }

    @After
    public void after(){
        hbaseClient.closeTable();
        hbaseClient.close();
    }


    public String randomNumber(String preNumber){
        return preNumber + String.format("%d", new Integer ((int)((Math.random() * 9 + 1) * 10000000)));
    }

    public String randomDate(int year){
        Random random = new Random();
        Calendar calendar = Calendar.getInstance();
        calendar.set(year,0,1,0,0,0);
        calendar.add(Calendar.MONTH, random.nextInt(12));
        calendar.add(Calendar.DAY_OF_MONTH, random.nextInt(31));
        calendar.add(Calendar.HOUR_OF_DAY, random.nextInt(24));
        calendar.add(Calendar.MINUTE, random.nextInt(60));
        calendar.add(Calendar.SECOND, random.nextInt(60));
        Date date = calendar.getTime();
        return DATE_FORMAT.format(date);
    }

    public String randomDate(int year,int month,int day){
        Random random = new Random();
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month-1, day,0,0,0);
        calendar.add(Calendar.HOUR_OF_DAY, random.nextInt(24));
        calendar.add(Calendar.MINUTE, random.nextInt(60));
        calendar.add(Calendar.SECOND, random.nextInt(60));
        Date date = calendar.getTime();
        return DATE_FORMAT.format(date);
    }

    @Test
    public void Test() throws ParseException {
        String number = randomNumber("183");
        System.out.println(number);
        String time = randomDate(2017);
        System.out.println(time);
        for (int i=0; i<100; i++){
            System.out.println(randomNumber("181"));
        }
    }

}

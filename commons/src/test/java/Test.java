import com.java.commons.util.DateUtil;
import com.java.commons.util.FileUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sgr on 2017/10/17/017.
 */
public class Test {
    public static void main(String[] args) {
        String time = DateUtil.getCurrentTime("yyyyMMddHHmmssSSS");
        System.out.println(time);

        System.out.println(DateUtil.getStandardCurrentTime());
        long start = System.currentTimeMillis();
        List<Object[]> list = new ArrayList<Object[]>();
        for (int i=0; i<1600000; i++){
            Object[] objects = new Object[5];
            objects[0] = "aaaa";
            objects[1] = Math.random();
            objects[2] = i;
            objects[3] = "a" + i;
            objects[4] = i*i;
            list.add(objects);
        }
        boolean b = FileUtils.writeFile(list,"C:\\Users\\Administrator\\Desktop\\data_160w.csv","|","utf8");
        System.out.println(b);
        long end = System.currentTimeMillis();
        System.out.println("cost: " + (end - start)/1000);
    }
}

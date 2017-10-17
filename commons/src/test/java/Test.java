import com.java.commons.util.DateUtil;

/**
 * Created by sgr on 2017/10/17/017.
 */
public class Test {
    public static void main(String[] args) {
        String time = DateUtil.getCurrentTime("yyyyMMddHHmmssSSS");
        System.out.println(time);

        System.out.println(DateUtil.getStandardCurrentTime());
    }
}

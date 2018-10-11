import java.util.HashMap;
import java.util.Map;

/**
 * Created by sgr on 2017/9/26.
 */
public class Test {
    private int i = 0;
    public static void main(String[] args) {
        Test t = new Test();
        System.out.println(t.i);
        System.out.println(" ".matches("(^[ ]*)"));
        System.out.println("3".hashCode());

        String s = "GSM_DROP_RATE_FM|WATCH_CW_USER_CNT|GSM_DROP_RATE_FZ|LTE_USER_CNT|TOTAL_USER_CNT|GSM_CONN_SUCC_RATE_FZ|GSM_CONN_SUCC_RATE_FM";
        String regex = "\\|";
        String[] ss = s.split(regex);
        System.out.println(ss[2]);
        Object o = "s";
        System.out.println();

        int i = Integer.MIN_VALUE;
        int i1 = -2147483648;
        int i2 = 2147483647;
        System.out.println(i);
        System.out.println(i1 - 1);
        System.out.println(i2 + 1);
        HashMap hashMap = new HashMap();
        hashMap.put('s','s');
        System.out.println(hashMap.put('s','a'));
        System.out.println(1 << 4);
        int i3 = 1 << 30;
        int[] a = {1,3};
        System.out.println(i3);

        Map<String,String> map = new HashMap<String, String>();
        map.put("a", "1");
        String a1 = map.get("1");
        System.out.println(a1);


        System.out.println("b"+10);
        System.out.println('b'+10);
    }
}

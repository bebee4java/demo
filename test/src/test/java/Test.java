/**
 * Created by sgr on 2017/9/26.
 */
public class Test {
    public static void main(String[] args) {
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
    }
}

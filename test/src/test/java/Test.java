import org.apache.commons.lang3.StringUtils;


/**
 * Created by sgr on 2017/9/26.
 */
public class Test {
    public static void main(String[] args) {
        System.out.println(StringUtils.join(new Object[]{"a","b","c"},'|'));

        String b = "20171025";
        String a = "20171026";
        System.out.println(Integer.parseInt(a) - Integer.parseInt(b) > 1);
    }
}

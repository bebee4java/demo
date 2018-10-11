import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.nashorn.internal.ir.ObjectNode;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.kitesdk.morphline.base.Fields;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * json解析
 *
 * @author sgr
 * @create 2018-10-10 18:46
 **/

public class JsonTest {

    public static void main(String[] args) {
        String message = "{\"name\": \"pp\", \"age\": 6, \"date\": \"2018-10-09 12:30:01\", \"sb\": true, \"parms\": {\"name2\": \"kaa\", \"age2\": 1}, \"id\": \"47\"}";
        if (message != null && !message.isEmpty()) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonMess = null;
            try {
                jsonMess = mapper.readTree(message);
                StringBuffer sb = new StringBuffer(2000);
                char separate_kv = 3;
                char separate_items = 2;
                Iterator<Map.Entry<String,JsonNode>> tags = jsonMess.fields();
                while (tags.hasNext()) {
                    Map.Entry<String,JsonNode> next = tags.next();
                    String k = next.getKey();
                    JsonNode value = next.getValue();
                    String v = null;
                    if (value.isTextual()){
                        v = value.asText();
                    }else {
                        v = value.toString();
                    }
                    sb.append(k).append(separate_kv).append(v).append(separate_items);
                    System.out.println(k);
                }
                message = sb.substring(0, sb.length()-1);
                System.out.println(message.length());
                System.out.println(message);





                String date_format = "yyyy-MM-dd HH:mm:ss";

                String date_str = "2018-10-09 12:30:01";
                Calendar calendar = Calendar.getInstance();

                if (date_str != null && !date_str.isEmpty()){
                    FastDateFormat sdf= FastDateFormat.getInstance(date_format);
                    Date date = null;
                    try {
                        date = sdf.parse(date_str);
                        calendar.setTime(date);
                    } catch (ParseException e) {
                        e.getMessage();
//                logger.info("date format error!date: {},record: {}",date_str, record);
                    }
                }

                int year = calendar.get(Calendar.YEAR);
                int month = calendar.get(Calendar.MONTH) + 1;
                int day = calendar.get(Calendar.DAY_OF_MONTH);
                int hour = calendar.get(Calendar.HOUR_OF_DAY);
                int minute = calendar.get(Calendar.MINUTE);


                String time = "2018-10-09 10:30:00";
                time = time.replace('-', '/').replace(':', '/').replace(' ', '/');

                String pt_day = time.substring(0, 10);
                String pt_hour = time.substring(0, 13);
                System.out.println(pt_day + " " + pt_hour );
            } catch (IOException e) {
                System.out.println("json format error message: {}" + message);
            }
        }
    }
}


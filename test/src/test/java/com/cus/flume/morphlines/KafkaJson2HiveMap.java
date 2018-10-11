package com.cus.flume.morphlines;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import com.fasterxml.jackson.databind.JsonNode;


public class KafkaJson2HiveMap {
    private static final Logger logger = LoggerFactory.getLogger(KafkaJson2HiveMap.class);
    private static final char SEPARATE_FIELDS = 1;
    private static final char SEPARATE_ITEMS = 2;
    private static final char SEPARATE_KV = 3;
    private static final ObjectMapper mapper = new ObjectMapper();

    public static boolean convert(Record record, Command child) {


        //获取消息对应的表名
        String topic = String.valueOf(record.getFirstValue("topic"));
        if (!record.getFields().containsKey("tb_" + topic)){
            logger.error("error record get table name Exception!! record: {}" , record);
            return false;
        }

        String table_name = String.valueOf(record.getFirstValue("tb_" + topic));
        if (table_name == null || table_name.isEmpty()){
            logger.error("error record unknown table name! record: {}", record);
            return false;
        }
        //获取消息

        String message = String.valueOf(record.getFirstValue(Fields.MESSAGE));
        if (message != null && !message.isEmpty()) {

            JsonNode jsonMess = null;

            try {
                jsonMess = mapper.readTree(message);
                // 时间处理 获取消息的时间
                if (!jsonMess.has("date_time")){
                    record.put(Fields.ATTACHMENT_BODY, "[lost date time]: " + message);
                    throw new Exception("error record lost date time! record: {} " + record);
                }
                //时间处理 组装pt_day
                String date_time = jsonMess.get("date_time").asText();
                String pt_day;
                if (date_time != null && date_time.length() >= 10){
                    String time = date_time.replace('-', '/').replace(':', '/').replace(' ', '/');
                    pt_day = time.substring(0, 10);
                    record.put("table_path", table_name+"/"+ pt_day);
                }else {
                    record.put(Fields.ATTACHMENT_BODY, "[date time exception]: " + message);
                    throw new Exception("error record date time exception! record: {} " +  record);
                }
                //组装map
                StringBuffer sb = new StringBuffer(2000);
                sb.append(date_time).append(SEPARATE_FIELDS);

                Iterator<Map.Entry<String,JsonNode>> tags = jsonMess.fields();
                while (tags.hasNext()) {
                    Map.Entry<String,JsonNode> next = tags.next();
                    String k = next.getKey();
                    if ("date_time".equals(k))
                        continue;

                    JsonNode value = next.getValue();
                    String v;
                    if (value.isTextual()){
                        v = value.asText();
                    }else {
                        v = value.toString();
                    }
                    sb.append(k).append(SEPARATE_KV).append(v).append(SEPARATE_ITEMS);
                }
                message = sb.substring(0, sb.length()-1);
                record.put(Fields.ATTACHMENT_BODY, message);

                logger.info("message: {}", message);
            } catch (IOException e) {
                logger.error("message json format error record: {}", record);
                logger.error(e.getMessage() , e);
                record.put(Fields.ATTACHMENT_BODY, "[json format error]: " + message);
                record.put("table_path", table_name+"/error");
            } catch (Exception e){
                logger.error(e.getMessage(), e);
                record.put("table_path", table_name+"/error");
            }
        } else {
            logger.error("error record message is null or empty! record: {}" , record);
            return false;
        }
        return child.process(record);
    }
}
package com.cus.flume.morphlines;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;

import java.io.IOException;
import java.util.*;

/**
 * kafka json数据转化成hive表数据,并返回表路径
 *
 * @author sgr
 * @create 2018-10-11 17:21
 **/

public class KafkaJson2HiveData implements CommandBuilder {
    private static final char SEPARATE_FIELDS = 1;
    private static final char SEPARATE_ITEMS = 2;
    private static final char SEPARATE_KV = 3;

    private static final ObjectMapper OBJECTMAPPER = new ObjectMapper();

    public KafkaJson2HiveData(){ }

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("kafkaJson2HiveData");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new KafkaJson2HiveData.JsonConvert(this, config, parent, child, context);
    }

    private static final class JsonConvert extends AbstractCommand {
        private static final Map<String, String> ttDict = new HashMap<String, String>();

        public JsonConvert(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            Config dict = getConfigs().getConfig(config, "tb_dict");
            for (Map.Entry<String, Object> entry : new Configs().getEntrySet(dict)) {
                ttDict.put(entry.getKey(), entry.getValue().toString());
            }
            validateArguments();
        }

        @Override
        protected boolean doProcess(Record record) {
            //获取消息对应的表名
            String topic = String.valueOf(record.getFirstValue("topic"));
            String table_name = ttDict.get("tb_"+topic);

            if (table_name == null){
                LOG.error("error record can't get table name form tb_dict!! record: {}" , record);
                return false;
            }
            if (!record.getFields().containsKey(Fields.MESSAGE)){
                LOG.warn("error record lost message field,kafka message maybe null record: {}" , record);
                return false;
            }
            //获取消息
            String message = String.valueOf(record.getFirstValue(Fields.MESSAGE));
            if (!message.trim().isEmpty()) {
                JsonNode jsonMess = null;
                try {
                    jsonMess = OBJECTMAPPER.readTree(message);
                    // 时间处理 获取消息的时间
                    if (!jsonMess.has("date_time")){
                        record.put(Fields.ATTACHMENT_BODY, "[lost date time]: " + record);
                        throw new Exception("error record lost date time! record: {} " + record);
                    }
                    //时间处理 组装pt_day
                    String date_time = jsonMess.get("date_time").asText();
                    String pt_day;
                    if (date_time != null && date_time.length() >= 10){
                        String time = date_time.replace('-', '/').replace(':', '/').replace(' ', '/');
                        pt_day = time.substring(0, 10);
                        record.put("table_path", table_name+"/"+pt_day);
                    }else {
                        record.put(Fields.ATTACHMENT_BODY, "[date time exception]: " + record);
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

                    LOG.info("message: {}", message);
                } catch (IOException e) {
                    LOG.error("message json format error record: {}", record);
                    LOG.error(e.getMessage() , e);
                    record.put(Fields.ATTACHMENT_BODY, "[json format error]: " + record);
                    record.put("table_path", table_name+"/error");
                } catch (Exception e){
                    LOG.error(e.getMessage(), e);
                    record.put("table_path", table_name+"/error");
                }
            } else {
                LOG.warn("error record message is empty! record: {}" , record);
                return false;
            }
            return super.doProcess(record);
        }

        @Override
        protected void doNotify(Record notification) {
            super.doNotify(notification);
        }
    }

}

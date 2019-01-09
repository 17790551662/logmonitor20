package orderMonitor.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import orderMonitor.domain.PaymentInfo;
import orderMonitor.utils.OrderMonitorHandler;

import java.util.List;

/**
 * Describe: 请补充类描述
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/23.
 */
public class SaveInfo2DB extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String firstField = input.getFields().get(0);
        if ("orderId".equals(firstField)) {
            OrderMonitorHandler.saveTrigger(input.getStringByField("orderId"), (List<String>)input.getValueByField("triggerList"));
        }
        if ("paymentInfo".equals(firstField)) {
            OrderMonitorHandler.savePaymentInfo((PaymentInfo) input.getValueByField("paymentInfo"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

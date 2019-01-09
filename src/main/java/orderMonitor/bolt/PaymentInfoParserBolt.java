package orderMonitor.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import orderMonitor.domain.PaymentInfo;
import orderMonitor.utils.OrderMonitorHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Describe: 解析订单信息
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/23.
 */
public class PaymentInfoParserBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        PaymentInfo paymentInfo = (PaymentInfo) input.getValueByField("paymentInfo");
        if (paymentInfo == null) {
            return;
        }
        List<String> triggerList = OrderMonitorHandler.match(paymentInfo);
//        List<String> triggerList = new ArrayList<>();
        triggerList.add("12");
        triggerList.add("13");
        if (triggerList.size() > 0) {
            collector.emit(new Values(paymentInfo.getOrderId(), triggerList));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderId", "triggerList"));
    }

}

package logAnalyze.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logAnalyze.storm.domain.LogMessage;
import logAnalyze.storm.utils.LogAnalyzeHandler;

/**
 * Describe: 请补充类描述
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/16.
 */
public class MessageFilterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //获取KafkaSpout发送出来的数据
        String line = input.getString(0);
        //对数据进行解析
        LogMessage logMessage = LogAnalyzeHandler.parser(line);
        if (logMessage == null || !LogAnalyzeHandler.isValidType(logMessage.getType())) {
            return;
        }
        collector.emit(new Values(logMessage.getType(), logMessage));
        //定时更新规则信息
        LogAnalyzeHandler.scheduleLoad();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //根据点击内容类型将日志进行区分
        declarer.declare(new Fields("type", "message"));
    }
}

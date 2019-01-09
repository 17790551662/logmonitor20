package orderMonitor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import orderMonitor.bolt.PaymentInfoParserBolt;
import orderMonitor.bolt.SaveInfo2DB;
import orderMonitor.spout.RandomSpout;

/**
 * Describe: 对用户订单进行分析，主要分析是否有欺诈行为。
 * 比如：
 * ip地址列表分析
 * 用户登陆设备列表分析
 * 用户平均下单时长分析、下单时间段分析、喜好品类、客单价等信息
 * 用户收货地址列表信息
 * 用户收货手机号码列表信息
 * 商品属性是否有易变现、高价值等特点
 * 支付是否选择货到付款、信用卡支付
 * <p>
 * 数据准备；
 * 订单基本信息，包括订单编号、订单价格、商品列表，以双十一消息为主
 * 基础数据：
 * 基础数据存放到redis中，需要设计相关的数据结构
 * 判断条件（触发规则）：
 * 规则1：商品属性：属于易变现，并且具有高价值的特点、
 * 用户属性：收货地址不在常用的收货地址中
 * 订单属性：订单总支付金额在2000以上
 * 规则2：商品属性：
 * 用户属性：不在常见ip地址内，近期修改过密码
 * 订单属性：
 * 判断基本流程：
 * 1，订单mq进来之后，对mq进行解析并校验所有的基础属性，生成一个规则结果数据
 * 2，对结果数据进行判断，生成触发规则的信息
 * 3，将触发规则信息回写到数据库
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/16.
 */
public class OrderMonitorTopologyMain {

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafka-spout", new RandomSpout(), 2);

        topologyBuilder.setBolt("paymentInfoParser-bolt", new PaymentInfoParserBolt(), 3);

        topologyBuilder.setBolt("SaveInfo2DB-bolt", new SaveInfo2DB(), 2)
                .fieldsGrouping("paymentInfoParser-bolt", new Fields("orderId"))
                .fieldsGrouping("kafka-spout", new Fields("paymentInfo"));

        //启动topology的配置信息
        Config topologConf = new Config();
        //TOPOLOGY_DEBUG(setDebug), 当它被设置成true的话， storm会记录下每个组件所发射的每条消息。
        //这在本地环境调试topology很有用， 但是在线上这么做的话会影响性能的。
//        topologConf.setDebug(true);
        //storm的运行有两种模式: 本地模式和分布式模式.
        if (args != null && args.length > 0) {
            //定义你希望集群分配多少个工作进程给你来执行这个topology
            topologConf.setNumWorkers(2);
            //向集群提交topology
            StormSubmitter.submitTopologyWithProgressBar(args[0], topologConf, topologyBuilder.createTopology());
        } else {
            topologConf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", topologConf, topologyBuilder.createTopology());
            Utils.sleep(10000000);
            cluster.shutdown();
        }

    }

}

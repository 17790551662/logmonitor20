package orderMonitor.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import orderMonitor.domain.PaymentInfo;
import orderMonitor.domain.Product;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * 随机产生消息发送出去
 */
public class RandomSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private TopologyContext context;
    private List<PaymentInfo> list;

    public RandomSpout() {
        super();
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        list = new ArrayList();

        List<Product> products = new ArrayList<>();
        //String id, String name, BigDecimal price, String catagory, long promotion, int num
        products.add(new Product("42077920512", "iphone6手机壳4.7透明超薄硅胶6s苹果", new BigDecimal(10), "配件", new BigDecimal(5.2), 1));
        products.add(new Product("42077920513", "iphone6钢化玻璃膜6s苹果6钢化保护", new BigDecimal(14), "配件", new BigDecimal(8), 1));
        products.add(new Product("42077920514", "iring苹果6s plus指环支架手机通用懒人", new BigDecimal(10), "配件", new BigDecimal(4), 1));
        products.add(new Product("42077920515", "苹果5s按键贴home键6plus识别指纹ip", new BigDecimal(8), "配件", new BigDecimal(5), 1));

//        String orderId, Date createOrderTime, String paymentId, Date paymentTime,
//                String shopId, String shopName, String shopMobile, String ip, String user,
//                String userMobile, String address, String addressCode,
//                String device, String orderType, List< Product > products, BigDecimal totalPrice
        PaymentInfo paymentInfo = new PaymentInfo("1280921834599179", new Date(), "2015112421001001660230384950", new Date()
                , "1997427721", "XXX飞韩专卖店", "18122299058", "192.168.121.11", "maoxiangyi"
                , "15652306418", "北京北京市昌平区回龙观建材城西路金燕龙办公楼", "12,12,35"
                , "xiaomi3", "121", products, new BigDecimal(19.80));
        list.add(paymentInfo);

    }

    @Override
    /**
     * 发送消息
     */
    public void nextTuple() {
        PaymentInfo paymentInfo = list.get(0);
        this.collector.emit(new Values(paymentInfo));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("paymentInfo"));
    }
}

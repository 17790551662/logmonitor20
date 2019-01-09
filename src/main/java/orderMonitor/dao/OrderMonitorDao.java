package orderMonitor.dao;

import orderMonitor.domain.Condition;
import orderMonitor.domain.PaymentInfo;
import orderMonitor.domain.Product;
import orderMonitor.domain.Trigger;
import orderMonitor.utils.DataSourceUtil;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Describe: 请补充类描述
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/25.
 */
public class OrderMonitorDao {
    private JdbcTemplate jdbcTemplate;

    public OrderMonitorDao() {
        jdbcTemplate = new JdbcTemplate(DataSourceUtil.getDataSource());
    }
    public List<Condition> loadRules() {
        String sql = "SELECT `id`,`name`,`ruleId`,`field`,`compare`,`value` " +
                "FROM `order_monitor`.`condition_order_monitor`";
        return jdbcTemplate.query(sql, new BeanPropertyRowMapper<Condition>(Condition.class));
    }

    public void saveTrigger(List<Trigger> list) {
        String sql = "INSERT " +
                "INTO `order_monitor`.`trigger_order_monitor` (`orderId`,`ruleId` ) " +
                "VALUES (?,?)";
        jdbcTemplate.batchUpdate(sql, list, list.size(), new ParameterizedPreparedStatementSetter<Trigger>() {
            @Override
            public void setValues(PreparedStatement ps, Trigger trigger) throws SQLException {
                ps.setString(1, trigger.getOrderId());
                ps.setInt(2, Integer.parseInt(trigger.getRuleId()));
            }
        });
    }

    public void savePayment(PaymentInfo paymentInfo) {
        savePaymentInfo(paymentInfo);
        saveProducts(paymentInfo.getOrderId(), paymentInfo.getProducts());
    }

    private void saveProducts(final String orderId, List<Product> products) {
        String sql = "INSERT " +
                "INTO `order_monitor`.`products_order_monitor` (`orderId`,`id`,`name`,`price`,`catagory`,`promotion`,`num`) " +
                "VALUES (?,?,?,?,?,?,?) ";
        jdbcTemplate.batchUpdate(sql, products, products.size(), new ParameterizedPreparedStatementSetter<Product>() {
            @Override
            public void setValues(PreparedStatement ps, Product product) throws SQLException {
                ps.setString(1, orderId);
                ps.setString(2, product.getId());
                ps.setString(3, product.getName());
                ps.setBigDecimal(4, product.getPrice());
                ps.setString(5, product.getCatagory());
                ps.setBigDecimal(6, product.getPromotion());
                ps.setInt(7, product.getNum());
            }
        });
    }

    private void savePaymentInfo(PaymentInfo paymentInfo) {
        String sql = "INSERT " +
                "INTO `order_monitor`.`paymentinfo_order_monitor` " +
                "(`orderId`,`createOrderTime`,`paymentId`,`paymentTime`," +
                "`shopId`,`shopName`,`shopMobile`,`ip`,`user`,`userMobile`," +
                "`address`,`addressCode`,`device`,`orderType`,`totalPrice`) " +
                "VALUES  (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
        jdbcTemplate.update(sql, paymentInfo.getOrderId(), paymentInfo.getCreateOrderTime(), paymentInfo.getPaymentId(), paymentInfo.getPaymentTime()
                , paymentInfo.getShopId(), paymentInfo.getShopName(), paymentInfo.getShopMobile(), paymentInfo.getIp(), paymentInfo.getUser(), paymentInfo.getUserMobile()
                , paymentInfo.getAddress(), paymentInfo.getAddressCode(), paymentInfo.getDevice(), paymentInfo.getOrderType(), paymentInfo.getTotalPrice());
    }

    public static void main(String[] args) {
        List<Trigger> list = new ArrayList<>();
        list.add(new Trigger("12121212", "11"));
        list.add(new Trigger("12132322", "12"));
        new OrderMonitorDao().saveTrigger(list);
        PaymentInfo paymentInfo = new PaymentInfo();
        paymentInfo.setPaymentId("121212121");
        paymentInfo.setOrderId("1212121121222121");
        paymentInfo.setCreateOrderTime(new Date());
        paymentInfo.setPaymentTime(new Date());
        paymentInfo.setShopName("爱我中华");
        paymentInfo.setTotalPrice(new BigDecimal(1000));
        new OrderMonitorDao().savePayment(paymentInfo);
    }
}

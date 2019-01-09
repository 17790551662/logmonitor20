package logAnalyze.storm.dao;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Describe: 请补充类描述
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/17.
 */
public class RecordBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    private Map<String, Map<String, Object>> appData;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {

    }

    @Override
    public int getBatchSize() {
        return appData.get("pv").size();
    }
}

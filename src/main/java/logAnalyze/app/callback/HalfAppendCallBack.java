package logAnalyze.app.callback;


import logAnalyze.app.domain.BaseRecord;
import logAnalyze.storm.dao.LogAnalyzeDao;
import logAnalyze.storm.utils.DateUtils;

import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Describe: 计算每半个小时的数据
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/17.
 */
public class HalfAppendCallBack implements Runnable {
    @Override
    public void run() {
        Calendar calendar = Calendar.getInstance();
        if (calendar.get(Calendar.MINUTE) % 30 == 0) {
            String endTime = DateUtils.getDataTime(calendar);
            String startTime = DateUtils.before30Minute(calendar);
            LogAnalyzeDao logAnalyzeDao = new LogAnalyzeDao();
            List<BaseRecord> baseRecordList = logAnalyzeDao.sumRecordValue(startTime, endTime);
            logAnalyzeDao.saveHalfAppendRecord(baseRecordList);
        }
    }
}

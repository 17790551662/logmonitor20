package logAnalyze.app.callback;

import logAnalyze.app.domain.BaseRecord;
import logAnalyze.storm.dao.LogAnalyzeDao;
import logAnalyze.storm.utils.DateUtils;

import java.util.Calendar;
import java.util.List;

/**
 * Describe: 计算每天的全量数据
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/17.
 */
public class DayAppendCallBack implements Runnable{
    @Override
    public void run() {
        Calendar calendar = Calendar.getInstance();
        if (calendar.get(Calendar.MINUTE) == 0 && calendar.get(Calendar.HOUR) == 0) {
            String endTime = DateUtils.getDataTime(calendar);
            String startTime = DateUtils.beforeOneDay(calendar);
            LogAnalyzeDao logAnalyzeDao = new LogAnalyzeDao();
            List<BaseRecord> baseRecordList = logAnalyzeDao.sumRecordValue(startTime, endTime);
            logAnalyzeDao.saveDayAppendRecord(baseRecordList);
        }
    }
}
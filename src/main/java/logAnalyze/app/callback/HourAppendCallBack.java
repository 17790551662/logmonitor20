package logAnalyze.app.callback;

import logAnalyze.app.domain.BaseRecord;
import logAnalyze.storm.dao.LogAnalyzeDao;
import logAnalyze.storm.utils.DateUtils;

import java.util.Calendar;
import java.util.List;

/**
 * Describe: 计算每小时的增量数据
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/17.
 */
public class HourAppendCallBack implements Runnable {
    @Override
    public void run()  {
        Calendar calendar = Calendar.getInstance();
        System.out.println(calendar.get(Calendar.MINUTE));
        if (calendar.get(Calendar.MINUTE) == 59) {
            //获取所有的增量数据
            String endTime = DateUtils.getDataTime(calendar);
            String startTime = DateUtils.beforeOneHour(calendar);
            LogAnalyzeDao logAnalyzeDao = new LogAnalyzeDao();
            List<BaseRecord> baseRecordList = logAnalyzeDao.sumRecordValue(startTime, endTime);
            logAnalyzeDao.saveHourAppendRecord(baseRecordList);
        }
    }
}
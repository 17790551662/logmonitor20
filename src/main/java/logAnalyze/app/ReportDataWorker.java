package logAnalyze.app;


import logAnalyze.app.callback.DayAppendCallBack;
import logAnalyze.app.callback.HalfAppendCallBack;
import logAnalyze.app.callback.HourAppendCallBack;
import logAnalyze.app.callback.OneMinuteCallBack;

import java.util.concurrent.*;

/**
 * Describe: 计算每个指标每分钟的增量数据
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/17.
 */
public class ReportDataWorker {

    public static void main(String[] args) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
//        //计算每分钟的增量数据，并将增量数据保存到mysql数据库中
        scheduledExecutorService.scheduleAtFixedRate(new OneMinuteCallBack(), 0, 60, TimeUnit.SECONDS);
        //计算每半个小时的增量数据，并将增量数据保存到mysql数据库中
//        scheduledExecutorService.scheduleAtFixedRate(new HalfAppendCallBack(), 0, 60, TimeUnit.SECONDS);
//        //计算每小时的增量数据，并将增量数据保存到mysql数据库中
//        scheduledExecutorService.scheduleAtFixedRate(new HourAppendCallBack(), 0, 60, TimeUnit.SECONDS);
////        //计算每天的全量，并将增量数据保存到mysql数据库中
//        scheduledExecutorService.scheduleAtFixedRate(new DayAppendCallBack(), 0, 60, TimeUnit.SECONDS);
    }

}

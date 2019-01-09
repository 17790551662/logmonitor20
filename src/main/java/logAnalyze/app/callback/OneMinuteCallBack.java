package logAnalyze.app.callback;

import logAnalyze.app.dao.CacheData;
import logAnalyze.app.domain.BaseRecord;
import logAnalyze.storm.dao.LogAnalyzeDao;
import logAnalyze.storm.domain.LogAnalyzeJob;
import logAnalyze.storm.utils.DateUtils;
import logAnalyze.storm.utils.MyShardedJedisPool;
import redis.clients.jedis.ShardedJedis;

import java.util.*;

/**
 * Describe: 计算每分钟的增量信息
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/17.
 */
public class OneMinuteCallBack implements Runnable {
    @Override
    public void run() {
        Calendar calendar = Calendar.getInstance();
        //24:00分时，将缓存清空
        if (calendar.get(Calendar.MINUTE) == 0 && calendar.get(Calendar.HOUR) == 24) {
            CacheData.setPvMap(new HashMap<String, Integer>());
            CacheData.setUvMap(new HashMap<String, Long>());
        }
        String date = DateUtils.getDate();
        //从redis中获取所有的指标最新的值
        List<BaseRecord> baseRecordList = getBaseRecords(date);
        //根据cacheData获取所有的指标的增量值  用最新全量值减去上一个时间段的全量值
        List<BaseRecord> appendDataList = getAppData(baseRecordList);
        //将增量数据保存到mysql中
        new LogAnalyzeDao().saveMinuteAppendRecord(appendDataList);
    }

    private List<BaseRecord> getAppData(List<BaseRecord> baseRecordList) {
        List<BaseRecord> appendDataList = new ArrayList<BaseRecord>();
        for (BaseRecord baseRecord : baseRecordList) {
            //用最新的pv减去缓存中的pv值，得到最新的增量数据
            int pvAppendValue = CacheData.getPv(baseRecord.getPv(), baseRecord.getIndexName());
            //用最新的pv减去缓存中的pv值，得到最新的增量数据
            long uvAppendValue = CacheData.getUv(baseRecord.getUv(), baseRecord.getIndexName());
            appendDataList.add(new BaseRecord(baseRecord.getIndexName(), pvAppendValue, uvAppendValue, baseRecord.getProcessTime()));
        }
        return appendDataList;
    }

    private List<BaseRecord> getBaseRecords(String date) {
        List<LogAnalyzeJob> logAnalyzeJobList = new LogAnalyzeDao().loadJobList();
        ShardedJedis jedis = MyShardedJedisPool.getShardedJedisPool().getResource();
        List<BaseRecord> baseRecords = new ArrayList<>();
        for (LogAnalyzeJob analyzeJob : logAnalyzeJobList) {
            String pvKey = "log:" + analyzeJob.getJobName() + ":pv:" + date;
            String uvKey = "log:" + analyzeJob.getJobName() + ":uv:" + date;
            String pv = jedis.get(pvKey);
            long uv = jedis.scard(uvKey);
            BaseRecord baseRecord = new BaseRecord(analyzeJob.getJobName(), Integer.parseInt(pv.trim()), uv, new Date());
            baseRecords.add(baseRecord);
        }
        return baseRecords;
    }
}

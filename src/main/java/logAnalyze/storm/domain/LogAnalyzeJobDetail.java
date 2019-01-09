package logAnalyze.storm.domain;

/**
 * Describe: 请补充类描述
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/16.
 */
public class LogAnalyzeJobDetail {
    private int id;
    private int jobId;
    private String field;
    private String value;
    private int compare;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getCompare() {
        return compare;
    }

    public void setCompare(int compare) {
        this.compare = compare;
    }

    @Override
    public String toString() {
        return "LogAnalyzeJobDetail{" +
                "id=" + id +
                ", jobId=" + jobId +
                ", field='" + field + '\'' +
                ", value='" + value + '\'' +
                ", compare=" + compare +
                '}';
    }
}

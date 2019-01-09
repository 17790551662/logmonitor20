package logAnalyze.storm.domain;

import java.io.Serializable;

/**
 * Describe: 根据用户行为记录的信息
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/10/13.
 */
public class LogMessage implements Serializable {
    private static final long serialVersionUID = 7270840760720823716L;
    private int type;//1：浏览日志、2：点击日志、3：搜索日志、4：购买日志
    private String hrefTag;//标签标识
    private String hrefContent;//标签对应的标识，主要针对a标签之后的内容
    private String referrerUrl;//来源网址
    private String requestUrl;//来源网址
    private String clickTime;//点击时间
    private String appName;//浏览器类型
    private String appVersion;//浏览器版本
    private String language;//浏览器语言
    private String platform;//操作系统
    private String screen;//屏幕尺寸
    private String coordinate;//鼠标点击时的坐标
    private String systemId; //产生点击流的系统编号
    private String userName;//用户名称


    public LogMessage(int type, String requestUrl, String referrerUrl, String userName) {
        this.type = type;
        this.requestUrl = requestUrl;
        this.referrerUrl = referrerUrl;
        this.userName = userName;
    }

    public String getCompareFieldValue(String field) {
        if ("hrefTag".equalsIgnoreCase(field)) {
            return hrefTag;
        } else if ("referrerUrl".equalsIgnoreCase(field)) {
            return referrerUrl;
        } else if ("requestUrl".equalsIgnoreCase(field)) {
            return requestUrl;
        }
        return "";
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getHrefTag() {
        return hrefTag;
    }

    public void setHrefTag(String hrefTag) {
        this.hrefTag = hrefTag;
    }

    public String getHrefContent() {
        return hrefContent;
    }

    public void setHrefContent(String hrefContent) {
        this.hrefContent = hrefContent;
    }

    public String getReferrerUrl() {
        return referrerUrl;
    }

    public void setReferrerUrl(String referrerUrl) {
        this.referrerUrl = referrerUrl;
    }

    public String getRequestUrl() {
        return requestUrl;
    }

    public void setRequestUrl(String requestUrl) {
        this.requestUrl = requestUrl;
    }

    public String getClickTime() {
        return clickTime;
    }

    public void setClickTime(String clickTime) {
        this.clickTime = clickTime;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getScreen() {
        return screen;
    }

    public void setScreen(String screen) {
        this.screen = screen;
    }

    public String getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(String coordinate) {
        this.coordinate = coordinate;
    }

    @Override
    public String toString() {
        return "LogMessage{" +
                "type='" + type + '\'' +
                ", hrefTag='" + hrefTag + '\'' +
                ", hrefContent='" + hrefContent + '\'' +
                ", referrerUrl='" + referrerUrl + '\'' +
                ", requestUrl='" + requestUrl + '\'' +
                ", clickTime='" + clickTime + '\'' +
                ", appName='" + appName + '\'' +
                ", appVersion='" + appVersion + '\'' +
                ", language='" + language + '\'' +
                ", platform='" + platform + '\'' +
                ", screen='" + screen + '\'' +
                ", coordinate='" + coordinate + '\'' +
                ", systemId='" + systemId + '\'' +
                '}';
    }
}

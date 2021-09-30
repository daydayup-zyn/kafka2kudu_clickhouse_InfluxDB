package com.zhbr.pojo;

public class SbgjPageVo {
	//告警编号
	private String alarmCode;
	//实时数据编号
	private String RTDCode;
	//告警时间
	private String alarmTime;
	//告警信息描述
	private String alarmContent;
	//越限类型
	private String alarmType;
	//监测设备名称
	private String objectName;
	//监测设备类型
	private String objectType;
	//操作
	private String wthDeal;
	//是否误报
	private String wthMis;
	//告警分析
	private String alarmAnalysis;
	//处理建议
	private String dealSuggestions;
	//是否保留
	private String wthRetain;
	//处理人姓名
	private String personName;
	//处理人部门
	private String personDept;
	//处理时间
	private String dealTime;
	//来源表名称
	private String srcTable;
	//监测点编号
	private String monitoringPointCode;
	//监测点类型
	private String monitoringPointType;
	//监测点名称
	private String monitoringPointName;
	//所属地市
	private String ssdsMc;
	//所属地市
	private String ssywdwMc;
	//当前值
	private String currentValue;
	//告警值
	private String alarmValue;
	//开始时间
	private String DSStartTime;
	//结束时间
	private String DSEndTime;
	//持续时间(分钟)
	private Integer minute;

	public String getAlarmCode() {
		return alarmCode;
	}

	public void setAlarmCode(String alarmCode) {
		this.alarmCode = alarmCode;
	}

	public String getRTDCode() {
		return RTDCode;
	}

	public void setRTDCode(String RTDCode) {
		this.RTDCode = RTDCode;
	}

	public String getAlarmTime() {
		return alarmTime;
	}

	public void setAlarmTime(String alarmTime) {
		this.alarmTime = alarmTime;
	}

	public String getAlarmContent() {
		return alarmContent;
	}

	public void setAlarmContent(String alarmContent) {
		this.alarmContent = alarmContent;
	}

	public String getAlarmType() {
		return alarmType;
	}

	public void setAlarmType(String alarmType) {
		this.alarmType = alarmType;
	}

	public String getObjectName() {
		return objectName;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	public String getObjectType() {
		return objectType;
	}

	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}

	public String getWthDeal() {
		return wthDeal;
	}

	public void setWthDeal(String wthDeal) {
		this.wthDeal = wthDeal;
	}

	public String getWthMis() {
		return wthMis;
	}

	public void setWthMis(String wthMis) {
		this.wthMis = wthMis;
	}

	public String getAlarmAnalysis() {
		return alarmAnalysis;
	}

	public void setAlarmAnalysis(String alarmAnalysis) {
		this.alarmAnalysis = alarmAnalysis;
	}

	public String getDealSuggestions() {
		return dealSuggestions;
	}

	public void setDealSuggestions(String dealSuggestions) {
		this.dealSuggestions = dealSuggestions;
	}

	public String getWthRetain() {
		return wthRetain;
	}

	public void setWthRetain(String wthRetain) {
		this.wthRetain = wthRetain;
	}

	public String getPersonName() {
		return personName;
	}

	public void setPersonName(String personName) {
		this.personName = personName;
	}

	public String getPersonDept() {
		return personDept;
	}

	public void setPersonDept(String personDept) {
		this.personDept = personDept;
	}

	public String getDealTime() {
		return dealTime;
	}

	public void setDealTime(String dealTime) {
		this.dealTime = dealTime;
	}

	public String getSrcTable() {
		return srcTable;
	}

	public void setSrcTable(String srcTable) {
		this.srcTable = srcTable;
	}

	public String getMonitoringPointCode() {
		return monitoringPointCode;
	}

	public void setMonitoringPointCode(String monitoringPointCode) {
		this.monitoringPointCode = monitoringPointCode;
	}

	public String getMonitoringPointType() {
		return monitoringPointType;
	}

	public void setMonitoringPointType(String monitoringPointType) {
		this.monitoringPointType = monitoringPointType;
	}

	public String getMonitoringPointName() {
		return monitoringPointName;
	}

	public void setMonitoringPointName(String monitoringPointName) {
		this.monitoringPointName = monitoringPointName;
	}

	public String getSsdsMc() {
		return ssdsMc;
	}

	public void setSsdsMc(String ssdsMc) {
		this.ssdsMc = ssdsMc;
	}

	public String getSsywdwMc() {
		return ssywdwMc;
	}

	public void setSsywdwMc(String ssywdwMc) {
		this.ssywdwMc = ssywdwMc;
	}

	public String getCurrentValue() {
		return currentValue;
	}

	public void setCurrentValue(String currentValue) {
		this.currentValue = currentValue;
	}

	public String getAlarmValue() {
		return alarmValue;
	}

	public void setAlarmValue(String alarmValue) {
		this.alarmValue = alarmValue;
	}

	public String getDSStartTime() {
		return DSStartTime;
	}

	public void setDSStartTime(String DSStartTime) {
		this.DSStartTime = DSStartTime;
	}

	public String getDSEndTime() {
		return DSEndTime;
	}

	public void setDSEndTime(String DSEndTime) {
		this.DSEndTime = DSEndTime;
	}

	public Integer getMinute() {
		return minute;
	}

	public void setMinute(Integer minute) {
		this.minute = minute;
	}
}

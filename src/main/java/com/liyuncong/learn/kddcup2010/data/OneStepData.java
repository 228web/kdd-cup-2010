package com.liyuncong.learn.kddcup2010.data;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class OneStepData  implements Serializable{
	private static final long serialVersionUID = -1053704371861409636L;
	public static final String SORTED_FIELD = "row,anonStudentId,unitName,sectionName,problemName,problemView,stepName,stepStartTime,firstTransactionTime,correctTransactionTime,stepEndTime,stepDurationSec,correctStepDurationSec,errorStepDurationSec,correctFirstAttempt,incorrects,hints,corrects,kcDefault,opportunityDefault";
	/**
	 * 需要给某些字段合理的默认值
	 */
	private double row;
	private String anonStudentId = "other";
	private String unitName = "other";
	private String sectionName = "other";
	private String problemName = "other";
	private double problemView = 0;
	private String stepName = "other";
	private String stepStartTime;
	private String firstTransactionTime;
	private String correctTransactionTime;
	private String stepEndTime;
	private double stepDurationSec = 0;
	private double correctStepDurationSec = 0;
	private double errorStepDurationSec = 0;
	private String correctFirstAttempt;
	private double incorrects = 0;
	private double hints = 0;
	private double corrects = 0;
	private String kcDefault = "other";
	private String opportunityDefault = "0";

	public double getRow() {
		return row;
	}

	public void setRow(double row) {
		this.row = row;
	}

	public String getAnonStudentId() {
		return anonStudentId;
	}

	public void setAnonStudentId(String anonStudentId) {
		this.anonStudentId = anonStudentId;
	}

	public String getUnitName() {
		return unitName;
	}

	public void setUnitName(String unitName) {
		this.unitName = unitName;
	}

	public String getSectionName() {
		return sectionName;
	}

	public void setSectionName(String sectionName) {
		this.sectionName = sectionName;
	}

	public String getProblemName() {
		return problemName;
	}

	public void setProblemName(String problemName) {
		this.problemName = problemName;
	}

	public double getProblemView() {
		return problemView;
	}

	public void setProblemView(double problemView) {
		this.problemView = problemView;
	}

	public String getStepName() {
		return stepName;
	}

	public void setStepName(String stepName) {
		this.stepName = stepName;
	}

	public String getStepStartTime() {
		return stepStartTime;
	}

	public void setStepStartTime(String stepStartTime) {
		this.stepStartTime = stepStartTime;
	}

	public String getFirstTransactionTime() {
		return firstTransactionTime;
	}

	public void setFirstTransactionTime(String firstTransactionTime) {
		this.firstTransactionTime = firstTransactionTime;
	}

	public String getCorrectTransactionTime() {
		return correctTransactionTime;
	}

	public void setCorrectTransactionTime(String correctTransactionTime) {
		this.correctTransactionTime = correctTransactionTime;
	}

	public String getStepEndTime() {
		return stepEndTime;
	}

	public void setStepEndTime(String stepEndTime) {
		this.stepEndTime = stepEndTime;
	}

	public double getStepDurationSec() {
		return stepDurationSec;
	}

	public void setStepDurationSec(double stepDurationSec) {
		this.stepDurationSec = stepDurationSec;
	}

	public double getCorrectStepDurationSec() {
		return correctStepDurationSec;
	}

	public void setCorrectStepDurationSec(double correctStepDurationSec) {
		this.correctStepDurationSec = correctStepDurationSec;
	}

	public double getErrorStepDurationSec() {
		return errorStepDurationSec;
	}

	public void setErrorStepDurationSec(double errorStepDurationSec) {
		this.errorStepDurationSec = errorStepDurationSec;
	}

	public String getCorrectFirstAttempt() {
		return correctFirstAttempt;
	}

	public void setCorrectFirstAttempt(String correctFirstAttempt) {
		this.correctFirstAttempt = correctFirstAttempt;
	}

	public double getIncorrects() {
		return incorrects;
	}

	public void setIncorrects(double incorrects) {
		this.incorrects = incorrects;
	}

	public double getHints() {
		return hints;
	}

	public void setHints(double hints) {
		this.hints = hints;
	}

	public double getCorrects() {
		return corrects;
	}

	public void setCorrects(double corrects) {
		this.corrects = corrects;
	}

	public String getKcDefault() {
		return kcDefault;
	}

	public void setKcDefault(String kcDefault) {
		this.kcDefault = kcDefault;
	}

	public String getOpportunityDefault() {
		return opportunityDefault;
	}

	public void setOpportunityDefault(String opportunityDefault) {
		this.opportunityDefault = opportunityDefault;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
	
	public static OneStepData parse(String line) throws NoSuchFieldException, SecurityException, NumberFormatException, IllegalArgumentException, IllegalAccessException {
		Objects.requireNonNull(line);
		OneStepData oneStepData = new OneStepData();
		Class<OneStepData> oneStepDataClass = (Class<OneStepData>) oneStepData.getClass();
		String[] elements = line.split("\t");
		String[] fieldNames = SORTED_FIELD.split(",");
		for(int i = 0, j = 0; i < fieldNames.length && j < elements.length; i++, j++) {
			String element = elements[j];
			if (j == 2 && i == 2) {
				element = elements[j].split(",")[0];
				j--;
			} else if (j == 2 && i == 3) {
				element = elements[j].split(",")[1];
			}
			Field field = oneStepDataClass.getDeclaredField(fieldNames[i]);
			field.setAccessible(true);
			Class fieldClass = field.getType();
			if (fieldClass.equals(double.class)) {
				Double value = parseDouble(element);
				if (value != null) {
					field.set(oneStepData, value);
				}
			} else {
				if (!"".equals(element.trim())) {
					field.set(oneStepData, dealWithArrtributeWithComma(element));
				}
			}
			
		}
		return oneStepData;
	}
	
	/**
	 * 
	 * @param element
	 * @return 如果element表示合法的整数，则返回对应的整数，否则返回null
	 */
	private static Double parseDouble(String element) {
		return "".equals(element) ? null : Double.parseDouble(element);
	}
	
	private static String dealWithArrtributeWithComma(String arrtribute) {
		if (arrtribute == null) {
			throw new NullPointerException();
		}
		if (StringUtils.isBlank(arrtribute)) {
			throw new IllegalArgumentException();
		}
		return arrtribute.replaceAll(",", " ");
	}
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queue;

/**
 *
 * @author rube
 */
public class QMessage {

    private String shortCode;
    private String msisdn;
    private String text;
    private String network;
    private String refNo;
    private String outboxId;

    private String created;
    private String modified;

    private String status;
    private long publishSequenceNumber;
    private Integer priority;

    private String outcomeSaveId;
    private String outcomeKey;
    private String outcomeValue;
    private String oddType;
    private String matchId;

    public QMessage(String shortCode, String text, Integer priority, String sdpId,
            String alertId, String network, String created, String modified) {
        this.shortCode = shortCode;
        this.text = text;
        this.network = network;
        this.created = created;
        this.modified = modified;

    }

    public QMessage(String outcomeSaveId, String oddType,
            String outcomeKey, String outcomeValue, String matchId) {
        this.outcomeSaveId = outcomeSaveId;
        this.outcomeKey = outcomeKey;
        this.outcomeValue = outcomeValue;
        this.oddType = oddType;
        this.matchId = matchId;
    }

    public QMessage() {

    }

    /**
     * @return the shortCode
     */
    public String getShortCode() {
        return shortCode;
    }

    /**
     * @param shortCode the shortCode to set
     */
    public void setShortCode(String shortCode) {
        this.shortCode = shortCode;
    }

    /**
     * @return the msisdn
     */
    public String getMsisdn() {
        return msisdn;
    }

    /**
     * @param msisdn the msisdn to set
     */
    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    /**
     * @return the text
     */
    public String getText() {
        return text;
    }

    /**
     * @param text the text to set
     */
    public void setText(String text) {
        this.text = text;
    }

    /**
     * @return the network
     */
    public String getNetwork() {
        return network;
    }

    /**
     * @param network the network to set
     */
    public void setNetwork(String network) {
        this.network = network;
    }

    /**
     * @return the refNo
     */
    public String getRefNo() {
        return refNo;
    }

    /**
     * @param refNo the refNo to set
     */
    public void setRefNo(String refNo) {
        this.refNo = refNo;
    }

    /**
     * @return the outboxesId
     */
    public String getOutboxId() {
        return outboxId;
    }

    /**
     * @param outboxesId the outboxesId to set
     */
    public void setOutboxId(String outboxId) {
        this.outboxId = outboxId;
    }

    /**
     * @return the created
     */
    public String getCreated() {
        return created;
    }

    /**
     * @param created the created to set
     */
    public void setCreated(String created) {
        this.created = created;
    }

    /**
     * @return the modified
     */
    public String getModified() {
        return modified;
    }

    /**
     * @param modified the modified to set
     */
    public void setModified(String modified) {
        this.modified = modified;
    }

    /**
     * @return the status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * @return the priority
     */
    public Integer getPriority() {
        return priority;
    }

    /**
     * @param priority the priority to set
     */
    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    /**
     * @return the publishSequenceNumber
     */
    public long getPublishSequenceNumber() {
        return publishSequenceNumber;
    }

    /**
     * @param publishSequenceNumber the publishSequenceNumber to set
     */
    public void setPublishSequenceNumber(long publishSequenceNumber) {
        this.publishSequenceNumber = publishSequenceNumber;
    }

    /**
     * @return the outcomeSaveId
     */
    public String getOutcomeSaveId() {
        return outcomeSaveId;
    }

    /**
     * @param outcomeSaveId the outcomeSaveId to set
     */
    public void setOutcomeSaveId(String outcomeSaveId) {
        this.outcomeSaveId = outcomeSaveId;
    }

    /**
     * @return the outcomeKey
     */
    public String getOutcomeKey() {
        return outcomeKey;
    }

    /**
     * @param outcomeKey the outcomeKey to set
     */
    public void setOutcomeKey(String outcomeKey) {
        this.outcomeKey = outcomeKey;
    }

    /**
     * @return the outcomeValue
     */
    public String getOutcomeValue() {
        return outcomeValue;
    }

    /**
     * @param outcomeValue the outcomeValue to set
     */
    public void setOutcomeValue(String outcomeValue) {
        this.outcomeValue = outcomeValue;
    }

    /**
     * @return the oddType
     */
    public String getOddType() {
        return oddType;
    }

    /**
     * @param oddType the oddType to set
     */
    public void setOddType(String oddType) {
        this.oddType = oddType;
    }

    /**
     * @return the matchId
     */
    public String getMatchId() {
        return matchId;
    }

    /**
     * @param matchId the matchId to set
     */
    public void setMatchId(String matchId) {
        this.matchId = matchId;
    }
}

/*
 */
package models;

/**
 *
 * @author rube
 */
public class QMessage {

    private String outcomeSaveId;
    private String outcomeKey;
    private String outcomeValue;
    private long publishSequenceNumber;
    private Integer priority;
    private String oddType;
    private String matchId;

    public QMessage(String outcomeSaveId, String oddType, String outcomeKey, String outcomeValue, String matchId) {
        this.outcomeSaveId = outcomeSaveId;
        this.outcomeKey = outcomeKey;
        this.outcomeValue = outcomeValue;
        this.oddType = oddType;
        this.matchId = matchId;
    }

    public QMessage() {

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

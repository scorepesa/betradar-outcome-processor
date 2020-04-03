/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package models;

import java.util.HashMap;
import org.json.JSONObject;
import utils.Constants.OutcomeType;

/**
 *
 * @author dennis
 */
public class OutcomeItem {

    private String outcomeValue;
    private HashMap<String, String> outcomeVoidValue;
    private String parentMatchID;
    private String liveBet;
    private String outcomeSubTypes;
    private JSONObject outcomeMSG;
    private OutcomeType outcomeType;
    private boolean won;
    private double voidFactor;
    private String subTypeId;
    private String specialBetValue;
    private String winningOutcome;

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
     * @return the parentMatchID
     */
    public String getParentMatchID() {
        return parentMatchID;
    }

    /**
     * @param parentMatchID the parentMatchID to set
     */
    public void setParentMatchID(String parentMatchID) {
        this.parentMatchID = parentMatchID;
    }

    /**
     * @return the liveBet
     */
    public String getLiveBet() {
        return liveBet;
    }

    /**
     * @param liveBet the liveBet to set
     */
    public void setLiveBet(String liveBet) {
        this.liveBet = liveBet;
    }

    /**
     * @return the outcomeVoidValue
     */
    public HashMap<String, String> getOutcomeVoidValue() {
        return outcomeVoidValue;
    }

    /**
     * @param outcomeVoidValue the outcomeVoidValue to set
     */
    public void setOutcomeVoidValue(HashMap<String, String> outcomeVoidValue) {
        this.outcomeVoidValue = outcomeVoidValue;
    }

    /**
     * @return the outcomeSubTypes
     */
    public String getOutcomeSubTypes() {
        return outcomeSubTypes;
    }

    /**
     * @param outcomeSubTypes the outcomeSubTypes to set
     */
    public void setOutcomeSubTypes(String outcomeSubTypes) {
        this.outcomeSubTypes = outcomeSubTypes;
    }

    /**
     * @return the outcomeType
     */
    public OutcomeType getOutcomeType() {
        return outcomeType;
    }

    /**
     * @param outcomeType the outcomeType to set
     */
    public void setOutcomeType(OutcomeType outcomeType) {
        this.outcomeType = outcomeType;
    }

    /**
     * @return the outcomeMSG
     */
    public JSONObject getOutcomeMSG() {
        return outcomeMSG;
    }

    /**
     * @param outcomeMSG the outcomeMSG to set
     */
    public void setOutcomeMSG(JSONObject outcomeMSG) {
        this.outcomeMSG = outcomeMSG;
    }

    /**
     * @return the won
     */
    public boolean isWon() {
        return won;
    }

    /**
     * @param won the won to set
     */
    public void setWon(boolean won) {
        this.won = won;
    }

    /**
     * @return the voidFactor
     */
    public double getVoidFactor() {
        return voidFactor;
    }

    /**
     * @param voidFactor the voidFactor to set
     */
    public void setVoidFactor(double voidFactor) {
        this.voidFactor = voidFactor;
    }

    public void setSubTypeId(String subTypeID) {
        this.subTypeId = subTypeID;
    }

    /**
     * @return the subTypeId
     */
    public String getSubTypeId() {
        return subTypeId;
    }

    /**
     * @return the specialBetValue
     */
    public String getSpecialBetValue() {
        return specialBetValue;
    }

    /**
     * @param specialBetValue the specialBetValue to set
     */
    public void setSpecialBetValue(String specialBetValue) {
        this.specialBetValue = specialBetValue;
    }

    /**
     * @return the winningOutcome
     */
    public String getWinningOutcome() {
        return winningOutcome;
    }

    /**
     * @param winningOutcome the winningOutcome to set
     */
    public void setWinningOutcome(String winningOutcome) {
        this.winningOutcome = winningOutcome;
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package models;

/**
 *
 * @author dennis
 */
public class OutboxItem {
    private int outboxID;
    private String winnerMsg;    

    /**
     * @return the outboxID
     */
    public int getOutboxID() {
        return outboxID;
    }

    /**
     * @param outboxID the outboxID to set
     */
    public void setOutboxID(int outboxID) {
        this.outboxID = outboxID;
    }

    /**
     * @return the winnerMsg
     */
    public String getWinnerMsg() {
        return winnerMsg;
    }

    /**
     * @param winnerMsg the winnerMsg to set
     */
    public void setWinnerMsg(String winnerMsg) {
        this.winnerMsg = winnerMsg;
    }
}

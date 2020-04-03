/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import java.util.ArrayList;
import java.util.HashMap;
import models.OutcomeItem;
import utils.Constants.OutcomeType;
import utils.Logging;

/**
 *
 * @author karuri
 */
public class ProcessMatchResult implements Runnable {

    private String winOutcomes;
    private OutcomeItem outcome;

    public ProcessMatchResult(String winOutcomes, OutcomeItem outcome) {
        this.winOutcomes = winOutcomes;
        this.outcome = outcome;
    }

    private String isJackpotResultQuery(String winOutcomes, OutcomeItem outcome) {
        String outcomeString = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();

        return "SELECT o.match_result_id,jt.sub_type_id FROM bet_slip s "
                + " inner join outcome o on (o.parent_match_id = s.parent_match_id "
                + " and o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value "
                + " and o.winning_outcome = s.bet_pick) INNER JOIN "
                + " jackpot_match jm ON o.parent_match_id = jm.parent_match_id INNER JOIN "
                + " jackpot_event je  ON je.jackpot_event_id = jm.jackpot_event_id INNER JOIN "
                + " jackpot_type jt ON jt.jackpot_type_id = je.jackpot_type WHERE "
                + " o.is_winning_outcome=1 and "
                //+ " CONCAT(o.winning_outcome,':',o.special_bet_value,':',jt.sub_type_id) "
                //+ " = '" + outcomeString + "' and "
                + " o.parent_match_id = " + outcome.getParentMatchID() + " "
                + " AND o.sub_type_id = jt.sub_type_id and o.live_bet = 0";
    }

    private  void isJackpotResult(String winOutcomes, OutcomeItem outcome) {
        try {
            if (outcome.getOutcomeType() == OutcomeType.NORMALOUTCOME) {
                String outcomesUpdateQuery = updateNormalOutcomeQuery(winOutcomes,
                        outcome);
                //Query to check whether its a jackpot outcome or not
                ArrayList<HashMap<String, String>> result = OutcomeProcessor.query(
                        isJackpotResultQuery(winOutcomes, outcome));
                if (result != null) {
                    if (!result.isEmpty()) {
                        //Jackpot result is present
                        Logging.info(" Jackpot result present parent_match_id = " + result.get(0).get("match_result_id") + " sub_type_id " + result.get(0).get("sub_type_id"));
                        //update outcome
                        updateOutcomes(outcomesUpdateQuery);
                        JackpotWinners jackpotWinners = new JackpotWinners();
                        int status = jackpotWinners.processJackpotBets();
                        Thread.currentThread().interrupt();
                    } else {
                        //update outcome     
                        Logging.info(" Jackpot result not present");
                        updateOutcomes(outcomesUpdateQuery);
                        Thread.currentThread().interrupt();
                    }
                } else {
                    //Not a Jackpot result                    
                    Logging.info(" Jackpot result not present");
                    //update outcome
                    updateOutcomes(outcomesUpdateQuery);
                    //updatePickResults(winOutcomes,outcome);
                    Thread.currentThread().interrupt();
                }
            } else if (outcome.getOutcomeType() == OutcomeType.OUTRIGHTOUTCOME) {
                //update Outright outcome
                Logging.info("Outright Outcome Present");
                String outcomesUpdateQuery = updateOutrightOutcomeQuery(winOutcomes, outcome);
                updateOutcomes(outcomesUpdateQuery);
                Thread.currentThread().interrupt();
            } else if (outcome.getOutcomeType() == OutcomeType.VIRTUALOUTCOME) {
                //update virtual outcome
                Logging.info("Virtual Outcome Present");
                String outcomesUpdateQuery = updateVirtualOutcomeQuery(winOutcomes, outcome);
                updateOutcomes(outcomesUpdateQuery);
                Thread.currentThread().interrupt();
            }
        } catch (Exception e) {
            Logging.error(ProcessMatchResult.class.getName() + " " + e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     *
     * Updates outcomes table status to 1 and returns result of updated record
     *
     * @param mysql Handles connections to MySql DB
     * @return String of the Updated record
     */
    private String updateOutcomes(String outcomesUpdateQuery) {
        try {
            String autoIncOutcomes = null;
            //outcomesUpdateQuery = updateOutcomeQuery(winOutcomes,outcome);
            autoIncOutcomes = OutcomeProcessor.update(outcomesUpdateQuery);
            if (autoIncOutcomes != null) {
                Logging.info("Outcome ID match_result_id =  " + autoIncOutcomes);
                System.out.println("Outcome ID match_result_id =  " + autoIncOutcomes);
                return autoIncOutcomes;
            } else {
                return outcomesUpdateQuery;
            }
        } catch (Exception ex) {
            Logging.error(ProcessOutcomes.class.getName() + " " + ex.getMessage(), ex);
            return outcomesUpdateQuery;
        }
    }

    private String updateNormalOutcomeQuery(String winOutcomes, OutcomeItem outcome) {
        String outcomeString = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();

        return "UPDATE outcome o SET o.match_result_id = LAST_INSERT_ID(o.match_result_id), "
                + "o.status = 1,o.modified = now() "
                + " WHERE o.parent_match_id = '" + outcome.getParentMatchID() + "' "
                + "AND o.live_bet = '" + outcome.getLiveBet() + "' AND "
                + "CONCAT(o.winning_outcome,':',o.special_bet_value,':',o.sub_type_id) "
                + " = '" + outcomeString + "' AND o.status <> 1";
    }

    private String updateOutrightOutcomeQuery(String winOutcomes, OutcomeItem outcome) {
        String outcomeString = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();

        return "UPDATE outright_outcome o SET o.outcome_id = LAST_INSERT_ID(o.outcome_id),"
                + "o.status = 2,o.modified = now() WHERE o.parent_outright_id = '" + outcome.getParentMatchID() + "'"
                + " AND CONCAT(o.betradar_competitor_id,':',o.special_bet_value,':') "
                + " =" + outcomeString + "'";
    }

    private String updateVirtualOutcomeQuery(String winOutcomes, OutcomeItem outcome) {
        String outcomeString = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();

        return " UPDATE virtual_outcome vo SET vo.v_match_result_id = LAST_INSERT_ID(vo.v_match_result_id),"
                + "vo.status = 1,vo.modified = now() WHERE vo.parent_virtual_id = '" + outcome.getParentMatchID() + "' "
                + "AND vo.live_bet = '" + outcome.getLiveBet() + "' AND "
                + "CONCAT(vo.winning_outcome,':',vo.special_bet_value,':',vo.sub_type_id) "
                + " ='" + outcomeString + "' AND vo.status <> 1";
    }

    @Override
    public void run() {
        isJackpotResult(winOutcomes, outcome);
    }
}

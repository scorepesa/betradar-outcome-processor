/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import java.util.ArrayList;
import java.util.HashMap;
import utils.Logging;

/**
 *
 * @author dennis
 */
public class JackpotWinners {

    /**
     * Instance of the MySQL connection pool.
     */
    public JackpotWinners() {

    }

    public int processJackpotBets() {
        int status = -1;
        try {
            ArrayList<HashMap<String, String>> resultOutcomes = OutcomeProcessor.query(
                    getGameOutcomes());
            if (resultOutcomes != null) {
                if (!resultOutcomes.isEmpty()) {
                    Logging.info("outcomes query okay");
                    for (int i = 0; i < resultOutcomes.size(); i++) {
                        String outcomesCount = resultOutcomes.get(i).get("outcomes_count");
                        String subTypeID = resultOutcomes.get(i).get("sub_type_id");
                        String jackpotEventID = resultOutcomes.get(i).get("jackpot_event_id");
                        String totalGames = resultOutcomes.get(i).get("total_games");
                        if (Integer.parseInt(outcomesCount) == Integer.parseInt(totalGames)) {
                            Logging.info("jackpotEventID " + jackpotEventID);
                            status = getUnProcessedWinners(jackpotEventID, subTypeID);
                        } else {
                            //send message to Joseph and me to result game
                            Logging.info("Not all outcomes resulted");
                            status = -1;
                        }
                    }
                } else {
                    //send message to me DB query not functioning
                    Logging.error("No Rows found in DB ");
                    status = -1;
                }
            } else {
                //send message to me DB not functioning
                Logging.error("DB connection failure");
                status = -1;
            }
        } catch (Exception ex) {
            Logging.error(JackpotWinners.class.getName() + " " + ex.getMessage(), ex);
        }
        return status;
    }

    private int getUnProcessedWinners(String jackpotEventID, String subTypeID) {
        int status = -1;
        try {
            ArrayList<HashMap<String, String>> jackpotBets = OutcomeProcessor.query(
                    getUnProcessedWinnersQuery(jackpotEventID));
            if (jackpotBets != null) {
                if (!jackpotBets.isEmpty()) {
                    Logging.info("bets query okay");
                    String betCount = jackpotBets.get(0).get("bet_count");
                    if (Integer.parseInt(betCount) == 0) {
                        Logging.info("bets query count = " + jackpotBets.get(0).get("bet_count"));
                        WinnersProcessor winnersProcessor = new WinnersProcessor();
                        status = winnersProcessor.processJackpotWinners(jackpotEventID, subTypeID);
                    } else {
                        //
                        Logging.error("Not all bet slips processed");
                        status = -1;
                    }
                } else {
                    //send message to me DB query not functioning
                    Logging.error("DB Query No rows found");
                    status = -1;
                }
            } else {
                //send message to me DB not functioning
                Logging.error("DB connection failure");
                status = -1;
            }
        } catch (Exception e) {
            Logging.error(JackpotWinners.class.getName() + " " + e.getMessage(), e);
        }
        return status;
    }

    private static String getGameOutcomes() {
        return "SELECT COUNT(match_result_id) AS outcomes_count,jee.jackpot_name,"
                + " jee.jackpot_event_id,jee.total_games,jt.sub_type_id FROM outcome o "
                + " inner join jackpot_match jmm on jmm.parent_match_id = o.parent_match_id "
                + " inner join jackpot_event jee "
                + " on jmm.jackpot_event_id = jee.jackpot_event_id inner join "
                + " jackpot_type jt on jt.jackpot_type_id = jee.jackpot_type "
                + " WHERE o.is_winning_outcome = 1 AND o.live_bet = 0 AND o.sub_type_id = jt.sub_type_id "
                + " AND o.parent_match_id IN (SELECT jm.parent_match_id FROM jackpot_match jm "
                + " INNER JOIN jackpot_event je  ON jm.jackpot_event_id = je.jackpot_event_id"
                + "  WHERE jm.status = 'INACTIVE' AND je.status = 'INACTIVE') "
                + " group by jee.jackpot_name,jt.sub_type_id";
    }

    private static String getUnProcessedWinnersQuery(String jackpotEventID) {
        return "SELECT COUNT(b.bet_id) AS bet_count FROM bet AS b "
                + " INNER JOIN jackpot_bet AS jb ON b.bet_id = jb.bet_id "
                + " INNER JOIN jackpot_event AS je ON jb.jackpot_event_id = je.jackpot_event_id "
                + " WHERE je.jackpot_event_id = '" + jackpotEventID + "'  AND b.status = 9 ";
    }
}

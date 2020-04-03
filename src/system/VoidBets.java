/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import models.OutcomeItem;
import org.json.JSONException;
import org.json.JSONObject;
import queue.Publisher;
import utils.Logging;

/**
 *
 * @author rube
 */
public class VoidBets implements Runnable {

    private final transient OutcomeItem outcome;
    private final transient DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public VoidBets(OutcomeItem outcome) {
        this.outcome = outcome;
    }

    //Create trx refund the money and log void_bet
    private boolean processVoidSlips() {
        boolean won = false;
        Logging.info("processVoidSlips Void Bet outcome: outcome =>" + outcome.getWinningOutcome()
                + ", VoidFactor =>" + outcome.getVoidFactor()
                + ", ParentMatchID => " + outcome.getParentMatchID());

        ArrayList<HashMap<String, String>> allSlipData = getBetSlipDetails();
        //Go on and award winning based on remaining games
        if (!allSlipData.isEmpty()) {
            ExecutorService es = Executors.newFixedThreadPool(allSlipData.size());
            for (HashMap<String, String> betData : allSlipData) {
                won = true;
                ProcessBets processBets = new ProcessBets(betData);
                es.submit(processBets);
            }
            es.shutdown();
        }
        return won;

    }

    private ArrayList<HashMap<String, String>> getBetSlipDetails() {
        String q = "SELECT b.bet_id,b.status,p.msisdn,b.profile_id,b.possible_win,b.bet_amount,"
                + " b.refund, b.is_void, b.revised_possible_win, bet_slip_id "
                + " FROM bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN profile p ON b.profile_id = p.profile_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value"
                + " and s.bet_pick = o.winning_outcome )"
                + " WHERE  s.parent_match_id = '" + outcome.getParentMatchID() + "' "
                + " AND s.live_bet = '" + outcome.getLiveBet() + "' "
                + " AND s.sub_type_id = '" + outcome.getSubTypeId() + "' "
                + " AND s.bet_pick = '" + outcome.getWinningOutcome() + "' "
                + " AND s.special_bet_value = '" + outcome.getSpecialBetValue() + "' "
                + " AND b.status NOT IN (5,7,24, 200) "
                + " AND s.total_games = (SELECT COUNT(IF(status=5 and win=1,1,null)) "
                + " FROM bet_slip WHERE bet_id = b.bet_id)";

        return OutcomeProcessor.query(q);

    }

    private void processOutcomes(JSONObject outcomes) {

        try {
            String query = null;
            if (outcome.getParentMatchID() != null) {
                query = updateVoidBetSlip(outcome.getParentMatchID(), outcome.getLiveBet());
            } else if (outcomes.has("parent_outright_id")
                    && !outcomes.isNull("parent_outright_id")) {
                query = updateVoidOutrights(outcome.getParentMatchID(),
                        outcome.getSpecialBetValue(),
                        outcome.getLiveBet(), outcome.getWinningOutcome());
            }

            String loosingBet = OutcomeProcessor.update(query);
            processVoidSlips();

        } catch (Exception e) {
            Logging.error(VoidBets.class.getName() + " " + e.getMessage(), e);
        }

    }

    private String updateVoidBetSlip(String parentMatchID, String liveBet) {
        return "UPDATE bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value "
                + " and o.winning_outcome = s.bet_pick ) "
                + " SET s.bet_slip_id = last_insert_id(bet_slip_id), "
                + " b.possible_win = cast(b.possible_win/(s.odd_value*" + outcome.getVoidFactor() + ") as decimal(10, 2)), "
                + " b.total_odd =  b.total_odd/(b.total_odd*" + outcome.getVoidFactor() + "), "
                + " b.refund = null, "
                + " b.is_void='" + (outcome.getVoidFactor() > 0 ? "1" : "0") + "', "
                + " s.status = " + (outcome.isWon() || outcome.getVoidFactor() == 1.0 ? " 5, " : " 3, ")
                + " s.win = " + (outcome.isWon() || outcome.getVoidFactor() == 1.0 ? " 1, " : " 0, ")
                + " s.modified = now(), "
                + " s.void_factor = '" + outcome.getVoidFactor() + "' "
                + " WHERE  s.parent_match_id = '" + parentMatchID + "' "
                + " AND s.live_bet =  '" + liveBet + "' "
                + " AND o.sub_type_id =  '" + outcome.getSubTypeId() + "' "
                + " AND o.special_bet_value =  '" + outcome.getSpecialBetValue() + "' "
                + " AND o.winning_outcome =  '" + outcome.getWinningOutcome() + "' "
                + " AND s.void_factor =0 "
                + " AND s.status NOT IN (5,24, 200) "
                + " AND b.status NOT IN (5,7,24, 200) ";
    }

    private String updateVoidOutrights(String parentMatchID, String specialBetValue,
            String liveBet, String betPick) {
        return "UPDATE bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value "
                + " and o.winning_outcome = s.bet_pick ) "
                + " SET s.bet_slip_id = last_insert_id(bet_slip_id), s.status = 3,s.win = 0,"
                + " b.status = 3,b.win = 0,s.modified = now()  WHERE o.is_winning_outcome = 0 and "
                + " s.parent_match_id = " + parentMatchID + ""
                + " AND s.live_bet =  " + liveBet + " "
                + " AND o.sub_type_id =  '" + outcome.getSubTypeId() + "' "
                + " AND o.special_bet_value =  '" + outcome.getSpecialBetValue() + "' "
                + " AND o.winning_outcome =  '" + outcome.getWinningOutcome() + "' "
                + " AND s.status NOT IN (5,24, 200) "
                + " AND b.status  NOT IN (5,7,24, 200)";
    }

    /**
     *
     * Inserts to outbox table the message to be sent and publishes to RabbitMQ
     *
     * @param profileID
     * @param msisdn
     * @param possibleWin
     * @param betID
     * @return id of the inserted record to outbox
     */
    private int insertToOutBox(String betID, String profileID, String msisdn, double refund) {
        int outboxID = 0;
        try {
            //ConstructLooser message
            String winnerMsg = "Sorry your betID " + betID + " did not succeed. "
                    + (refund > 0 ? " You have been refunded KSH. " + refund + " for void game " : "")
                    + "Place another bet to win with ALUBET 29992.";
            //Gets shortcode from conf file
            //String shortCode = OutcomeProcessor.getNetwork(msisdn);
            //Gets network operating the number passed as an argument
            String network = OutcomeProcessor.getNetwork(OutcomeProcessor.getValidPrefix(msisdn));
            //Gets current date
            String insertToOutBox = "INSERT INTO outbox (outbox_id,shortcode,network,profile_id,"
                    + "date_created,date_sent,`text`,msisdn) values (LAST_INSERT_ID(outbox_id),"
                    + " 101010,'"
                    + network + "'," + profileID + ",now(),now(),'" + winnerMsg + "','" + msisdn + "')";
            String outboxmsg = OutcomeProcessor.update(insertToOutBox);
            try {
                outboxID = Integer.parseInt(outboxmsg);
            } catch (NumberFormatException e) {
                Logging.error("Number format exception thrown converting to integer", e);
            } catch (Exception e) {
                Logging.error("Error converting to integer", e);
            }
        } catch (Exception e) {
            Logging.error(WinnersProcessor.class.getName() + " " + e.getMessage(), e);
        }
        return outboxID;
    }

    private void pushToQueue(int outBoxID, String msisdn, String betID, double refund) {
        Date date = new Date();
        String winnerMsg = "Try again. Your betID " + betID + " did not succeed. "
                + (refund > 0 ? " You have beed refunded KSH. " + refund + " for void games " : "")
                + "Place another bet to win with ALUBET 29992.";

        String network = OutcomeProcessor.getNetwork(msisdn);
        if (outBoxID != 0) {
            Logging.info("Outbox ID " + outBoxID);
            System.err.println("Outbox ID " + outBoxID);
            try {
                long tStart = System.currentTimeMillis();
                JSONObject json = new JSONObject();
                json.put("msisdn", msisdn);
                json.put("text", winnerMsg);
                json.put("shortCode", "101010");
                json.put("bet_id", betID);
                json.put("network", network);
                json.put("reference_no", msisdn + "_" + outBoxID);
                json.put("date_created", dateFormat.format(date));
                json.put("date_modified", dateFormat.format(date));
                //TODO: Bull shit this into a config
                json.put("exchange", "ALUBET_WINNER_MESSAGES_QUEUE");

                Publisher.publishMessage(json.toString(), true);
                long tEnd = System.currentTimeMillis();
                long tDelta = tEnd - tStart;
                double elapsedSeconds = tDelta / 1000.0;
                Logging.info("Time taken to publish to queue + " + elapsedSeconds);
            } catch (IOException | JSONException ex) {
                Logging.error("Error publishing message: " + ex.getMessage(), ex);
            }
        }
    }

    @Override
    public void run() {
        try {
            processOutcomes(outcome.getOutcomeMSG());
        } catch (Exception ex) {
            Logging.error(VoidBets.class.getName() + " " + ex.getMessage(), ex);
        }
    }

}

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
public class LoosingBets implements Runnable {

    private final transient OutcomeItem outcome;
    private transient DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public LoosingBets(OutcomeItem outcome) {
        this.outcome = outcome;
    }

    private void reverseRefund(String profileId, String betId, String betSlipId,
            double refundAmount, int totalGames) {
        Logging.info("Just in case we already refunded =>" + outcome.getWinningOutcome()
                + ", VoidFactor =>" + outcome.getVoidFactor());

        String a = "select id from transaction where reference = '" + betId + "."
                + betSlipId + "' limit 1";
        ArrayList<HashMap<String, String>> r = OutcomeProcessor.query(a);
        if (r.isEmpty()) {
            Logging.info("No refund was given ignore trx ...BETID " + betId);
            return;
        }

        if (totalGames > 1) {
            String trxQ = "insert into transaction (id, profile_id, account,"
                    + " iscredit,reference,amount,running_balance, created_by, "
                    + " created,modified, status) "
                    + " values (null,'" + profileId + "',  '" + profileId + "_VIRTUAL', "
                    + " 0, '" + betId + "." + betSlipId + "' , '" + refundAmount + "',  null, 'OutcomeProcessor', "
                    + " now(), now(), 'COMPLETE');";

            String vtrx = OutcomeProcessor.update(trxQ);
            if (vtrx != null) {
                String profileQ = "update profile_balance set balance = balance-" + refundAmount + " where "
                        + " profile_id = '" + profileId + "' limit 1";
                OutcomeProcessor.update(profileQ);

            }
        }

    }

    //Create trx refund the money and log void_bet
    private boolean updateVoidBet(String profileId, String betId, String betSlipId,
            double refundAmount, int totalGames) {
        boolean won = false;
        Logging.info("Void Bet outcome: Lossing outcome =>" + outcome.getWinningOutcome()
                + ", VoidFactor =>" + outcome.getVoidFactor());

        //Update betslip as won so we process the other way round
        String sql = "update bet_slip inner join bet using(bet_id) "
                + " set bet_slip.status=5, bet_slip.win=1,"
                + " bet.revised_possible_win = cast(if(bet.revised_possible_win is null, "
                + " bet.possible_win/bet_slip.odd_value, "
                + " bet.revised_possible_win/bet_slip.odd_value) as decimal(10,2)) where "
                + " bet_slip.bet_slip_id = '" + betSlipId + "'; ";
        String vtrx = OutcomeProcessor.update(sql);
        //reset bet_slip status on non loose bets
        String sqlB = "select count(if(status in (0,1) , 1, null))un_processed,  "
                + " count(if(status=3, 1, null))lost, count(if(status=5, 1, null))won  "
                + " from bet_slip "
                + " where bet_id='" + betId + "'";

        ArrayList<HashMap<String, String>> lostBets = OutcomeProcessor.query(sqlB);
        if (lostBets.isEmpty()) {
            Logging.info("Unable to find Lost bets IGONRING VOID");
            return false;
        }
        HashMap<String, String> lb = lostBets.get(0);
        if (Integer.valueOf(lb.get("lost")) != 0) {
            Logging.info("Lost bet_slip ignore bets IGONRING VOID");
            return false;
        } else {
            if (Integer.valueOf(lb.get("un_processed")) != 0
                    && Integer.valueOf(lb.get("lost")) == 0) {
                Logging.info("Upprocessed bets ignore bets RESETTING BET STATUS " + betId);
                String sqlBetU = "update bet set status=1, win=0 where bet_id = '" + betId + "'";
                String vtrx1 = OutcomeProcessor.update(sqlBetU);
                return false;
            }
        }
        if (Integer.valueOf(lb.get("won")) == totalGames) {
            //Won all games
            won = true;
            Logging.info("ALL slips won for bet_id " + betId);
            String sqlBetA = "update bet set status=5, win=1 where bet_id = '" + betId + "'";
            String vtrx1 = OutcomeProcessor.update(sqlBetA);
        }

        HashMap<String, String> betData = getBetSlipDetails(betSlipId);
        //Go on and award winning based on remaining games
        if (!betData.isEmpty()) {
            ProcessBets processBets = new ProcessBets(betData);
            ExecutorService es = Executors.newSingleThreadExecutor();
            es.submit(processBets);
            es.shutdown();
        }
        //Attempt refund  amount which was already refunded
        reverseRefund(profileId, betId, betSlipId, refundAmount, totalGames);
        return won;

    }

    private HashMap<String, String> getBetSlipDetails(String betSlipId) {
        String q = "SELECT b.bet_id,b.status,p.msisdn,b.profile_id,b.possible_win,b.bet_amount,"
                + " b.refund, b.is_void, b.revised_possible_win, bet_slip_id "
                + " FROM bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN profile p ON b.profile_id = p.profile_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value"
                + " and s.bet_pick = o.winning_outcome )"
                + " WHERE s.bet_slip_id = '" + betSlipId + "' "
                + " AND b.status =5 "
                + " AND s.total_games = (SELECT COUNT(IF(status=5 and win=1,1,null)) "
                + " FROM bet_slip WHERE bet_id = b.bet_id)";

        ArrayList<HashMap<String, String>> rs = OutcomeProcessor.query(q);
        return rs.isEmpty()
                ? new HashMap<String, String>()
                : rs.get(0);

    }

    private void processOutcomes(JSONObject outcomes) {
        int status = -1;
        while (status == -1) {
            try {
                String query = null;
                if (outcome.getParentMatchID() != null) {
                    query = updateLoosingBetSlip(outcome.getParentMatchID(),
                            outcome.getSubTypeId(), outcome.getSpecialBetValue(),
                            outcome.getLiveBet(), outcome.getWinningOutcome());
                } else if (outcomes.has("parent_outright_id")
                        && !outcomes.isNull("parent_outright_id")) {
                    query = updateLoosingOutrights(outcome.getParentMatchID(),
                            outcome.getSpecialBetValue(),
                            outcome.getLiveBet(), outcome.getWinningOutcome());
                }
                //ArrayList<HashMap<String, String>> firstTimeLoosers
                //        = this.getFirstTimeLoosers();

                String loosingBet = processLoosingBets(query);
                if (loosingBet != null) {
                    status = 1;
                    //Do not send sms to loosers
                    //this.sendSMSToFirsttimeLoosers(firstTimeLoosers);

                }

            } catch (Exception e) {
                Logging.error(LoosingBets.class.getName() + " " + e.getMessage(), e);
            }
        }
    }

    private ArrayList<HashMap<String, String>> getFirstTimeLoosers() {
        String sql = "select bs.bet_slip_id, bet.bet_id, bet.total_odd, "
                + " bs.odd_value, bs.total_games, "
                + " p.profile_id, bet.bet_amount, (select count(bet_slip_id) "
                + " from bet_slip where status=3 and bet_id = bs.bet_id) as already_lost, "
                + " p.msisdn, network from bet_slip bs inner join bet "
                + " on bet.bet_id = bs.bet_id inner join profile p on p.profile_id "
                + " = bet.profile_id inner join outcome o on bs.parent_match_id = "
                + " o.parent_match_id and bs.sub_type_id = o.sub_type_id and "
                + " bs.special_bet_value = o.special_bet_value and "
                + " bs.bet_pick = o.winning_outcome where o.is_winning_outcome=0 "
                + " and bet.status in (1, 400) "
                + " and o.parent_match_id = '" + outcome.getParentMatchID() + "'  "
                + " and o.sub_type_id = '" + outcome.getSubTypeId() + "' and "
                + " o.special_bet_value = '" + outcome.getSpecialBetValue() + "' and "
                + " o.winning_outcome = '" + outcome.getWinningOutcome() + "'";
        return OutcomeProcessor.query(sql);
    }

    private void sendSMSToFirsttimeLoosers(ArrayList<HashMap<String, String>> loosers) {

        for (HashMap<String, String> looser : loosers) {
            String profileId = looser.get("profile_id");
            String msisdn = looser.get("msisdn");
            String betId = looser.get("bet_id");
            double betAmount = Double.valueOf(looser.get("bet_amount"));
            //String oddValue = looser.get("odd_value");
            //double totalOdd = Double.valueOf(looser.get("total_odd"));
            //int lost = Integer.valueOf(looser.get("already_lost"));
            int totalGames = Integer.valueOf(looser.get("total_games"));
            String betSlipId = looser.get("bet_slip_id");
            boolean won = false;

            double possibleWin = 0; //taxablePossibleWin, tax;
            //Void factor 1 refund entire stake
            //Void factor 05 refund half stake loose half
            if (outcome.getVoidFactor() > 0) {
                if (outcome.getVoidFactor() == 1.0) {
                    possibleWin = betAmount / totalGames;
                    //taxablePossibleWin = 0;
                    //tax = 0;
                    won = updateVoidBet(profileId, betId, betSlipId,
                            possibleWin, totalGames);
                } else if (outcome.getVoidFactor() == 0.5) {
                    possibleWin = (betAmount / totalGames) / 2;
                    //taxablePossibleWin = 0;
                    //tax = 0;
                    won = updateVoidBet(profileId, betId, betSlipId,
                            possibleWin, totalGames);
                }

            }
            if (won) {
                //DONOT: Fucking send loosing SMS
                return;
            }

            int outboxID = insertToOutBox(betId, profileId, msisdn, 0);

            if (outboxID != 0) {
                Logging.info(" Looser outbox ID " + String.valueOf(outboxID));
                try {
                    pushToQueue(outboxID, msisdn, betId, 0);
                } catch (Exception exe) {

                }
            }
        }

    }

    private  String processLoosingBets(String query) {
        String loosingBetID = null;
        try {
            loosingBetID = OutcomeProcessor.update(query);
        } catch (Exception ex) {
            Logging.error(LoosingBets.class.getName() + " " + ex.getMessage(), ex);
        }
        return loosingBetID;
    }

    private String updateLoosingBetSlip(String parentMatchID,
            String subTypeID, String specialBetValue, String liveBet, String betPick) {
        return "UPDATE bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value "
                + " and o.winning_outcome = s.bet_pick ) "
                + " SET s.bet_slip_id = last_insert_id(bet_slip_id), b.status = 3,"
                + " s.status = 3,b.win = 0,s.win = 0,s.modified = now(), "
                + " s.void_factor = '" + outcome.getVoidFactor() + "', "
                + " b.refund = null, "
                + " b.is_void='" + (outcome.getVoidFactor() > 0 ? "1" : "0") + "' WHERE "
                + " o.is_winning_outcome = 0 and "
                + " s.parent_match_id = '" + parentMatchID + "' "
                + " AND s.live_bet =  '" + liveBet + "' "
                + " AND o.sub_type_id =  '" + outcome.getSubTypeId() + "' "
                + " AND o.special_bet_value =  '" + outcome.getSpecialBetValue() + "' "
                + " AND o.winning_outcome =  '" + outcome.getWinningOutcome() + "' "
                + " AND s.status NOT IN (5,24, 200) AND b.status NOT IN (5,7,24, 200) ";
    }

    private String getTwinOutcomes(String parentMatchID, String subTypeID,
            String specialBetValue, String liveBet) {
        return "SELECT GROUP_CONCAT('''',winning_outcome,'''') winning_outcomes,"
                + " COUNT(winning_outcome) outcomes_count FROM outcome WHERE "
                + " parent_match_id = " + parentMatchID + " "
                + " AND sub_type_id = " + subTypeID + " "
                + " AND live_bet = " + liveBet + " AND "
                + " special_bet_value = '" + specialBetValue + "'";
    }

    private String updateLoosingOutrights(String parentMatchID, String specialBetValue,
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
                + " AND s.status NOT IN (5,24, 200) AND b.status "
                + " NOT IN (5,7,24, 200)";
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
            String winnerMsg = "Bet ID " + betID + " not successful. "
                    + (refund > 0 ? " KSH " + refund + " refunded for cancelled game  " : "")
                    + " SMS ALB to 29992 to get new games "
                    + " place another bet and win with ALUBET";
            //Gets shortcode from conf file
            //String shortCode = OutcomeProcessor.getNetwork(msisdn);
            //Gets network operating the number passed as an argument
            String network = OutcomeProcessor.getNetwork(OutcomeProcessor.getValidPrefix(msisdn));
            //Gets current date
            String insertToOutBox = "INSERT INTO outbox (outbox_id,shortcode,network,profile_id,"
                    + "date_created,date_sent,`text`,msisdn) values (LAST_INSERT_ID(outbox_id),"
                    + " 290050,'"
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
        String winnerMsg = "Bet ID " + betID + " not successful. "
                    + (refund > 0 ? " KSH " + refund + " refunded for cancelled game  " : "")
                    + " SMS ALB to 29992 to get new games "
                    + " place another bet and win with ALUBET";

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
            Logging.error(LoosingBets.class.getName() + " " + ex.getMessage(), ex);
        }
    }

}

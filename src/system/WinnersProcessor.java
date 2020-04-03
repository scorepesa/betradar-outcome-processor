/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import db.MySQL3;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.json.JSONObject;
import queue.Publisher;
import queue.QMessage;
import utils.Logging;
import utils.Props;

/**
 *
 * @author dennis
 */
public class WinnersProcessor {

    private transient DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public WinnersProcessor() {
    }

    public int processJackpotWinners(String jackpotEventID, String subTypeID) {
        int status = -1;
        String jackpotWinnerMsg = "Jackpot Winners" + "\n\n";
        String subject = "Jackpot Results for Jackpot event " + jackpotEventID;
        try {
            String newWinners = OutcomeProcessor.update(insertSelectWinnersQuery(jackpotEventID));
            if (newWinners != null) {
                //do all else
                ArrayList<HashMap<String, String>> winners = OutcomeProcessor.query(
                        MySQL3.getConnection(), selectWinnersQuery(jackpotEventID));
                if (!winners.isEmpty()) {
                    for (int i = 0; i < winners.size(); i++) {
                        String profileID = winners.get(i).get("profile_id");
                        String betID = winners.get(i).get("bet_id");
                        String msisdn = winners.get(i).get("msisdn");
                        int outboxID = insertToOutBox(betID, profileID, msisdn);

                        if (outboxID != 0) {
                            System.out.println(" outbox ID " + String.valueOf(outboxID));
                            Logging.info(" outbox ID " + String.valueOf(outboxID));
                            try {
                                String winnerMsg = jackpotWinnerMsg(betID);
                                String msg = URLEncoder.encode(winnerMsg, "UTF-8");

                                Logging.error(" Failed to Insert to Kannel DB ");
                                pushToQueue(outboxID, msisdn, betID);

                            } catch (UnsupportedEncodingException ex) {
                                Logging.error(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                            }
                        }
                    }
                } else {
                    Logging.info(" No winners found ");
                }
                status = updateJackpotBetsEvent(jackpotEventID);
                if (status != -1) {
                    String msg = jackpotWinnerMsg
                            + getJackpotWinnersCount(jackpotEventID)
                            + getJackpotOutcomes(jackpotEventID, subTypeID);
//                    SendJackpotResults sendJackpotResults = new SendJackpotResults();
//                    sendJackpotResults.sendMail(msg, subject);
                }
            } else {
                status = updateJackpotBetsEvent(jackpotEventID);
                if (status != -1) {
                    String msg = jackpotWinnerMsg
                            + getJackpotWinnersCount(jackpotEventID)
                            + getJackpotOutcomes(jackpotEventID, subTypeID);
//                    SendJackpotResults sendJackpotResults = new SendJackpotResults();
//                    sendJackpotResults.sendMail(msg, subject);
                }
            }
        } catch (SQLException e) {
            Logging.error(WinnersProcessor.class.getName() + " " + e.getMessage(), e);
        }
        return status;
    }

    private static String jackpotWinnerMsg(String betID) {
        return Props.getJackpotWinnerMessage()[0] + " " + betID + " "
                + Props.getJackpotWinnerMessage()[1];
    }

    private String getJackpotOutcomes(String jackpotEventID, String subTypeID) {
        String jackpotOutcomeMsg = "";
        try {
            ArrayList<HashMap<String, String>> jackpotGameOutcomes
                    = OutcomeProcessor.query(jackpotGameOutcomesQuery(jackpotEventID, subTypeID));
            if (jackpotGameOutcomes != null) {
                if (!jackpotGameOutcomes.isEmpty()) {
                    String winningCombination = "Winning combination" + "\t";
                    for (int i = 0; i < jackpotGameOutcomes.size(); i++) {
                        jackpotOutcomeMsg = jackpotOutcomeMsg
                                + jackpotGameOutcomes.get(i).get("game_order") + "\t"
                                + jackpotGameOutcomes.get(i).get("winning_outcome") + "\t"
                                + jackpotGameOutcomes.get(i).get("home_team") + " vs "
                                + jackpotGameOutcomes.get(i).get("away_team") + "\n";
                        winningCombination = winningCombination + jackpotGameOutcomes.get(i).get("winning_outcome");
                    }

                    jackpotOutcomeMsg = jackpotOutcomeMsg + "\n" + winningCombination;
                }
            }
        } catch (Exception exp) {
            Logging.error("error getting jackpot outcomes " + exp.getMessage(), exp);
        }
        return jackpotOutcomeMsg;
    }

    private String getJackpotWinnersCount(String jackpotEventID) {
        int[] res = null;
        String jackpotWinnerMsg = "";
        try {
            ArrayList<HashMap<String, String>> jackpotWinnersCount
                    = OutcomeProcessor.query(jackpotWinnersCountQuery(jackpotEventID));

            if (jackpotWinnersCount != null) {
                if (!jackpotWinnersCount.isEmpty()) {
                    Logging.info(" winners found ");
                    for (int i = 0; i < jackpotWinnersCount.size(); i++) {
                        jackpotWinnerMsg = jackpotWinnerMsg + "Bets with "
                                + jackpotWinnersCount.get(i).get("total_games_correct")
                                + "/" + jackpotWinnersCount.get(i).get("total_games") + " "
                                + "=" + " " + jackpotWinnersCount.get(i).get("winner_count") + "\n";
                    }
                } else {
                    Logging.info(" No winners found ");
                    jackpotWinnerMsg = jackpotWinnerMsg + " No Winning Bets " + "\n";
                }
            } else {
                Logging.info(" No winners found ");
                jackpotWinnerMsg = jackpotWinnerMsg + " No Winning Bets " + "\n";
            }
        } catch (Exception exp) {
            Logging.error("error getting jackpot outcomes " + exp.getMessage(), exp);
        }
        return jackpotWinnerMsg + "\n";
    }

    private int updateJackpotBetsEvent(String jackpotEventID) {
        int status = -1;
        try {
            String updatedJackpotBetsEvents = OutcomeProcessor.update(updateJackpotBetsEventsQuery(jackpotEventID));
            System.err.println("event is " + updatedJackpotBetsEvents);
            if (updatedJackpotBetsEvents != null) {
                Logging.info("updated jackpot event and jack pot bet");
                System.err.println("updated jackpot event and jack pot bet");
                status = 1;
            } else {
                System.err.println("jackpot event and jack pot bet not updated");
                Logging.info("jackpot event and jack pot bet not updated");
                status = -1;
            }
        } catch (Exception ex) {
            Logging.error(WinnersProcessor.class.getName() + " " + ex.getMessage(), ex);
        }
        return status;
    }

    private String getJackpotWinnersQuery(String jackpotEventID) {
        return "SELECT b.bet_id,b.status,p.msisdn,b.profile_id,b.created,b.win,je.requisite_wins,je.total_games, "
                + " (SELECT COUNT(IF(status=5 and win=1,1,null)) FROM bet_slip WHERE bet_id = b.bet_id) AS games_won "
                + " FROM  bet AS b INNER JOIN jackpot_bet AS jb ON b.bet_id = jb.bet_id "
                + " INNER JOIN jackpot_event AS je ON je.jackpot_event_id = jb.jackpot_event_id "
                + " INNER JOIN profile AS p ON b.profile_id = p.profile_id "
                + " WHERE je.jackpot_event_id = '" + jackpotEventID + "' AND jb.status = 'ACTIVE' "
                + " AND (SELECT COUNT(IF(status=5 and win=1,1,null)) FROM bet_slip WHERE bet_id = b.bet_id) >= je.requisite_wins "
                + " ORDER BY games_won DESC";
    }

    private String insertSelectWinnersQuery(String jackpotEventID) {
        return "INSERT INTO jackpot_winner (win_amount,bet_id,jackpot_event_id, msisdn,"
                + "total_games_correct,created_by,status,created,modified) SELECT 0,"
                + "b.bet_id," + jackpotEventID + ", p.msisdn,(SELECT COUNT(IF(status=5 AND "
                + "win=1,1,NULL)) FROM bet_slip WHERE bet_id = b.bet_id) AS games_won,"
                + "'JackpotWinnersProcessor',0,NOW(),NOW()  FROM  bet AS b INNER JOIN "
                + "jackpot_bet AS jb ON b.bet_id = jb.bet_id INNER JOIN jackpot_event AS "
                + "je ON je.jackpot_event_id = jb.jackpot_event_id INNER JOIN profile AS "
                + "p ON b.profile_id = p.profile_id  WHERE je.jackpot_event_id = "
                + "" + jackpotEventID + " AND jb.status = 'ACTIVE' AND (SELECT "
                + "COUNT(IF(status=5 and win=1,1,null)) FROM bet_slip WHERE bet_id = "
                + "b.bet_id) >= je.requisite_wins ORDER BY games_won DESC";
    }

    private String selectWinnersQuery(String jackpotEventID) {
        return "SELECT j.bet_id,p.profile_id,p.msisdn FROM  profile p INNER JOIN "
                + "jackpot_winner j ON p.msisdn = j.msisdn INNER JOIN jackpot_event je "
                + "ON j.jackpot_event_id = je.jackpot_event_id WHERE "
                + "j.jackpot_event_id = " + jackpotEventID + " AND "
                + "j.total_games_correct = je.total_games";
    }

    private String updateJackpotBetsEventsQuery(String jackpotEventID) {
        return "UPDATE jackpot_bet jb inner join jackpot_event je ON "
                + "jb.jackpot_event_id = je.jackpot_event_id SET "
                + "jb.jackpot_bet_id = LAST_INSERT_ID(jb.jackpot_bet_id),"
                + "je.jackpot_event_id = LAST_INSERT_ID(je.jackpot_event_id),"
                + "je.status = 'FINISHED',jb.status = 'FINISHED' WHERE "
                + "je.jackpot_event_id = '" + jackpotEventID + "'";
    }

    private static String jackpotWinnersCountQuery(String jackpotEventID) {
        return "SELECT jw.total_games_correct,COUNT(jackpot_winner_id) winner_count,"
                + "je.total_games FROM jackpot_winner jw INNER JOIN jackpot_event je ON "
                + "jw.jackpot_event_id = je.jackpot_event_id WHERE "
                + "je.jackpot_event_id = '" + jackpotEventID + "' GROUP BY "
                + "total_games_correct ORDER BY total_games_correct DESC";
    }

    private static String jackpotGameOutcomesQuery(String jackpotEventID, String subTypeID) {
        return "SELECT jm.game_order,m.home_team,m.away_team,o.winning_outcome FROM "
                + "`match` m INNER JOIN outcome o ON m.parent_match_id = "
                + "o.parent_match_id INNER JOIN jackpot_match jm ON m.parent_match_id ="
                + " jm.parent_match_id WHERE o.sub_type_id = '" + subTypeID + "' AND "
                + "jm.jackpot_event_id = '" + jackpotEventID + "' ORDER BY jm.game_order ASC";
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
    private int insertToOutBox(String betID, String profileID, String msisdn) {
        int outboxID = 0;
        try {
            //Construct Winner Congratulatory Message
            String winnerMsg = jackpotWinnerMsg(betID);
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

    private void pushToQueue(int outBoxID, String msisdn, String betID) {
        String winnerMsg = jackpotWinnerMsg(betID);
        Date date = new Date();
        String network = OutcomeProcessor.getNetwork(msisdn);
        QMessage qMessage = null;
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
                System.err.println("Time taken to publish to queue + " + elapsedSeconds);
            } catch (IOException ex) {
                System.err.println("Error publishing message: " + ex);
                Logging.error("Error publishing message: " + ex.getMessage(), ex);
                updateRetryOutbox(String.valueOf(outBoxID));
            } catch (Exception ex) {
                System.err.println("Error publishing message: " + ex);
                Logging.error("Error publishing message: " + ex.getMessage(), ex);
            }
        }
    }

    private String updateRetryOutbox(String outBoxID) {
        try {
            String updateOutBox = "UPDATE outbox SET LAST_INSERT_ID(outbox_id),retry_status = 1 WHERE outbox_id = '" + outBoxID + "'";
            outBoxID = OutcomeProcessor.update(MySQL3.getConnection(), updateOutBox);
            Logging.info("Outbox ID retry status " + outBoxID);
            System.out.println("Outbox ID retry status " + outBoxID);
        } catch (SQLException ex) {
            Logging.error(WinnersProcessor.class.getName() + " " + ex.getMessage(), ex);
        } catch (Exception e) {
            Logging.error(WinnersProcessor.class.getName() + " " + e.getMessage(), e);
        }
        return outBoxID;
    }
}

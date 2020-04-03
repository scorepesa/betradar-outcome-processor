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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.Callable;
import models.OutboxItem;
import org.json.JSONException;
import org.json.JSONObject;
import queue.Publisher;
import utils.Constants;
import utils.Logging;
import utils.Props;

/**
 * Processes bets once Games are completed Gets completed matches and updates
 * their status to 4 meaning payments are being processed Gets all bets placed
 * in a particular match updates the winning bets and inserts to winners table,
 * inserts into transactions table and outboxes table finally it publishes the
 * messages into a queue
 *
 *
 * @author dennis
 * @version 1.0
 */
public class ProcessBets implements Callable<String> {

    /**
     * Instance of the MySQL connection pool.
     */
    private final DecimalFormat df = new DecimalFormat("#.##");
    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private transient HashMap<String, String> matchBets;
    private transient ArrayList<String> sqlStatements = new ArrayList<String>();

    /**
     *
     * Constructor
     *
     * @param matchBets
     */
    public ProcessBets(HashMap<String, String> matchBets) {
        this.matchBets = matchBets;
    }

    /**
     *
     * updates winning bet_slip records
     *
     * @param mysql Handles connections to MySql DB
     * @param results A result of all the winning bet slips
     * @return an Object of WinBet which contains the winning bet and winning
     * bet slip
     */
    private synchronized String processWinningBetSlip(HashMap<String, String> results) {
        Logging.info("bet ids " + results.toString());
        String profileID, betID, possibleWin, msisdn, status, betAmount;
        if (!results.isEmpty()) {
            msisdn = results.get("msisdn");
            profileID = results.get("profile_id");
            betID = results.get("bet_id");
            possibleWin = results.get("possible_win");

            betAmount = results.get("bet_amount");
            status = results.get("status");
            if (checkPossibleWin(possibleWin, betAmount, status)) {
                if (!status.equals("9")) {
                    try {
                        if (doDBTransactions(MySQL3.getConnection(),
                                updateProfileBalanceQuery(profileID, possibleWin, betID),
                                insertAccounts(profileID, betID, possibleWin))) {
                            OutboxItem outboxItem = msgToSend(results);
                            Logging.info("OutBox ID is " + outboxItem.getOutboxID());
                            int outboxID = outboxItem.getOutboxID();
                            if (outboxID != 0) {
                                try {
                                    String winnerMsg = outboxItem.getWinnerMsg();
                                    String msg = URLEncoder.encode(winnerMsg, "UTF-8");

                                    Logging.info(" For the sake of @Imani send SMS via API ");
                                    pushToQueue(outboxID, msisdn, winnerMsg, betID);

                                } catch (UnsupportedEncodingException ex) {
                                    Logging.error(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                                }
                            }
                            return betID;
                        }
                    } catch (SQLException ex) {
                        Logging.error(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                    }
                } else {
                    try {
                        if (OutcomeProcessor.update(MySQL3.getConnection(), updateWinningBet(results.get("bet_id"), "5")) != null) {
                            Logging.info("Jackpot won" + betID);
                            return betID;
                        }
                    } catch (SQLException ex) {
                        Logging.error(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                    }
                }
            } else {
                try {
                    if (OutcomeProcessor.update(MySQL3.getConnection(), updateWinningBet(results.get("bet_id"), "7")) != null) {
                        Logging.info("Bet ID Over 100000 " + betID);
                        return betID;
                    }

                } catch (SQLException ex) {
                    Logging.error(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                }
            }
        }
        return null;
    }

    /**
     *
     * Checks whether possible win exceeds 50000 Kshs
     *
     * @param possibleWin the win amount for the bet
     * @return boolean false if possible win exceeds 50000
     */
    private synchronized boolean checkPossibleWin(String possibleWin, String betAmount,
            String status) {
        boolean isPossibleWin = true;
        Logging.info("Bet Amount" + betAmount);
        Logging.info("Possible" + possibleWin);
        double checkWins = Double.parseDouble(possibleWin) - Double.parseDouble(betAmount);
        if (checkWins > Props.getStatus7()) {
            if (!status.equals("9")) {
                if (!status.equals("2")) {
                    isPossibleWin = false;
                    Logging.info("isPossibleWin " + isPossibleWin);
                    System.err.println("isPossibleWin  " + isPossibleWin);
                }
            }
        }
        return isPossibleWin;
    }

    private synchronized boolean totalAmountWonPerDay(String profileID) {
        boolean lessThanMillion = true;
        try {
            ArrayList<HashMap<String, String>> result = OutcomeProcessor.query(MySQL3.getConnection(),
                    totalAmountWonPerDayQuery(profileID));
            if (result != null) {
                if (!result.isEmpty()) {
                    int amountWon = Integer.parseInt(result.get(0).get("total_amount"));
                    if (amountWon > 1000000) {
                        lessThanMillion = false;
                    }
                }
            }
        } catch (Exception se) {
            Logging.error(ProcessBets.class.getName() + " Exception thrown", se);
        }
        return lessThanMillion;
    }

    /**
     *
     * Creates a query that updates bet Table with won bets
     *
     * @param msSql Handles connections to MySql DB
     * @param winBet String of the winning bet pick
     * @param matchID query of the winning bet
     * @return Query that will update the winning bet
     */
    private synchronized String updateWinningBet(String bet_id, String status) {
        String betUpdateQuery = "UPDATE bet set bet_id = LAST_INSERT_ID(bet_id),win = 1,status = " + status + ",modified = now()"
                + " WHERE bet_id = " + bet_id
                + " AND status <> 5 "
                + " AND win = 0";
        Logging.info("betID id " + bet_id);
        System.err.println("betID id  " + bet_id);
        return betUpdateQuery;
    }

    /**
     *
     * Updates users profiles balance
     *
     * @param profileId the winners profile_id
     * @param possibleWin the amount the winner has won
     * @return String of the query that updates the user profile balance
     */
    private synchronized String updateProfileBalanceQuery(String profileId, String possibleWin, String betID) {
        return "UPDATE profile_balance pb INNER JOIN bet b ON pb.profile_id = b.profile_id "
                + "SET b.status = 5,b.win = 1,pb.balance = (balance + '" + possibleWin + "') WHERE b.bet_id = '" + betID + "'; ";
    }

    /**
     *
     * Creates query that Inserts into transactions table updating both users
     * also be updated.
     *
     * @param profileID ProfileID for account being credited
     * @param possibleWin Amount to be credited to the account
     * @param betID The winning betID that has caused the crediting
     * @return Query that inserts into transaction account
     */
    private synchronized String insertAccounts(String profileID, String betID, String possibleWin) {
        return "INSERT INTO transaction (profile_id,account,iscredit,reference,amount,created_by,created,modified) VALUES "
                + " ('" + profileID + "','" + profileID + "_VIRTUAL','1','" + betID + "','" + possibleWin + "','OutcomProcessor',now(),now()); ";
    }

    /**
     *
     * @param profileId punter profileID
     * @return Query that will get the profile balance and bonus balance
     */
    private synchronized String getBalance(String profileId) {
        return "SELECT balance,bonus_balance FROM profile_balance WHERE "
                + "profile_id = '" + profileId + "' LIMIT 1";
    }

    private synchronized String getNextGameQuery() {
        return "SELECT em.home_team,em.away_team,em.game_id,em.start_time,eo.odd_key,"
                + "eo.odd_value FROM event_odd eo INNER JOIN `match` em ON "
                + "eo.parent_match_id = em.parent_match_id INNER JOIN competition c ON "
                + "em.competition_id = c.competition_id WHERE eo.sub_type_id = 10 AND "
                + "em.start_time > NOW() AND c.sport_id = '14' AND eo.odd_key IS NOT "
                + "NULL AND eo.odd_value IS NOT NULL ORDER BY em.start_time ASC LIMIT 3";
    }

    /**
     *
     * @param betID
     * @return
     */
    private synchronized String checkVoidGames(String betID) {
        return " SELECT GROUP_CONCAT(game_id SEPARATOR ', ') game_id "
                + " FROM `match` m INNER JOIN bet_slip s ON "
                + " m.parent_match_id = s.parent_match_id WHERE "
                + " s.sub_type_id = -1 AND s.bet_pick = -1 AND s.bet_id = " + betID;
    }

    /**
     *
     * @param shortCode
     * @param network
     * @param profileID
     * @param winnerMsg
     * @param msisdn
     * @return
     */
    private synchronized String insertToOutBox(String shortCode, String network, String profileID, String winnerMsg, String msisdn) {
        return "INSERT INTO outbox (outbox_id,shortcode,network,profile_id,"
                + "date_created,date_sent,`text`,msisdn) values (LAST_INSERT_ID(outbox_id)," + shortCode + ",'"
                + network + "'," + profileID + ",now(),now(),'" + winnerMsg + "','" + msisdn + "')";

    }

    private synchronized String totalAmountWonPerDayQuery(String profileID) {
        return "SELECT SUM(amount) total_amount FROM transaction WHERE iscredit = 1 AND"
                + " DATE(created) = CURDATE() AND created_by IN ('OutcomesConsumer'"
                + ",'VirtualOutcomesConsumer','GR') AND profile_id = "
                + "'" + profileID + "'";
    }

    /**
     *
     * @param outBoxID
     * @param msisdn
     * @param winnerMsg
     * @param betID
     * @return
     */
    private synchronized String pushToQueue(int outBoxID,
            String msisdn, String winnerMsg, String betID) {
        Date date = new Date();
        String network = OutcomeProcessor.getNetwork(msisdn);
        if (outBoxID != 0 & winnerMsg != null) {
            Logging.info("Outbox ID " + outBoxID);
            System.err.println("Outbox ID " + outBoxID);
            try {
                long tStart = System.currentTimeMillis();
                JSONObject json = new JSONObject();
                json.put("msisdn", msisdn);
                json.put("text", winnerMsg);
                json.put("shortCode", "29992");
                json.put("bet_id", betID);
                json.put("network", network);
                json.put("reference_no", msisdn + "_" + outBoxID);
                json.put("date_created", dateFormat.format(date));
                //TODO: Bull shit this into a config
                json.put("exchange", "ALUBET_WINNER_MESSAGES_QUEUE");
                Logging.info("Collected SMS form JSON SEND " + json.toString());
                Publisher.publishMessage(json.toString(), true);
                long tEnd = System.currentTimeMillis();
                long tDelta = tEnd - tStart;
                double elapsedSeconds = tDelta / 1000.0;
                System.err.println("Time taken to publish to queue + " + elapsedSeconds);
                return betID;
            } catch (IOException ex) {
                ex.printStackTrace();
                Logging.error("Error publishing message: " + ex.getMessage(), ex);
                updateRetryOutbox(String.valueOf(outBoxID));
                return betID;
            } catch (JSONException ex) {
                ex.printStackTrace();
                Logging.error("WinBets Threw error ", ex);
                return betID;
            }
        } else {
            return null;
        }
    }

    /**
     *
     * @param outBoxID
     * @return
     */
    private synchronized String updateRetryOutbox(String outBoxID) {
        try {
            String updateOutBox = "UPDATE outbox SET retry_status = 1 WHERE outbox_id = '" + outBoxID + "'";
            outBoxID = OutcomeProcessor.update(MySQL3.getConnection(), updateOutBox);
            Logging.info("Outbox ID retry status " + outBoxID);
            System.out.println("Outbox ID retry status " + outBoxID);
            return outBoxID;
        } catch (SQLException ex) {
            Logging.error(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
            return null;
        }
    }

    /**
     *
     * @param connection
     * @param sqlStatements1
     * @param sqlStatements2
     * @return
     */
    private synchronized boolean doDBTransactions(Connection connection,
            String sqlStatements1, String sqlStatements2) {
        boolean dbTransactions = false;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = connection;
            long tStart = System.currentTimeMillis();
            conn.setAutoCommit(false);

            stmt = conn.createStatement();
            stmt.executeUpdate(sqlStatements1, Statement.RETURN_GENERATED_KEYS);
            Logging.info(sqlStatements1);
            stmt.executeUpdate(sqlStatements2, Statement.RETURN_GENERATED_KEYS);
            Logging.info(sqlStatements2);
            rs = stmt.getGeneratedKeys();

            conn.commit();
            long tEnd = System.currentTimeMillis();
            long tDelta = tEnd - tStart;
            double elapsedSeconds = tDelta / 1000.0;
            Logging.info("Time taken to execute DB transaction + " + elapsedSeconds);
            dbTransactions = true;
            rs.close();
            stmt.close();
        } catch (SQLException se) {
            Logging.error(ProcessBets.class.getName() + " exception thrown ", se);
            OutcomeProcessor.writeToFile(Constants.FAILED_QUERIES_FILE, sqlStatements1 + " " + sqlStatements2, true);
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                System.err.println(ex.getMessage());
                Logging.error("DB Transaction Roll back Exception Thrown", ex);
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logging.fatal(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logging.fatal(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    Logging.fatal(ProcessBets.class.getName() + " " + ex.getMessage(), ex);
                }
            }
        }
        return dbTransactions;
    }

    /**
     *
     * @param queryBalance
     * @return
     */
    private synchronized String[] customerBalance(String queryBalance) {
        String[] customerBalance = null;
        try {
            ArrayList<HashMap<String, String>> result = OutcomeProcessor.query(MySQL3.getConnection(), queryBalance);
            if (result != null) {
                if (!result.isEmpty()) {
                    String[] cb = {result.get(0).get("balance"), result.get(0).get("bonus_balance")};
                    customerBalance = cb;
                }
            }
        } catch (Exception se) {
            Logging.error(ProcessBets.class.getName() + " Exception thrown", se);
        }
        return customerBalance;
    }

    private synchronized String getNextGame() {
        String nextGame = null;
        try {
            ArrayList<HashMap<String, String>> result = OutcomeProcessor.query(
                    MySQL3.getConnection(), getNextGameQuery());
            if (result != null) {
                if (!result.isEmpty()) {
                    nextGame = result.get(0).get("home_team") + " vs "
                            + result.get(0).get("away_team") + " Game ID: "
                            + result.get(0).get("game_id") + " ";
                    for (int i = 0; i < result.size(); i++) {
                        nextGame = nextGame + result.get(i).get("odd_key") + "="
                                + result.get(i).get("odd_value") + ", ";
                    }
                }
            }
        } catch (Exception e) {
            Logging.error(ProcessBets.class.getName() + " Exception thrown", e);
        }
        return nextGame;
    }

    /**
     *
     * @param checkVoidGames
     * @return
     */
    private synchronized String getVoidGames(String checkVoidGames) {
        String voidGames = "";
        try {
            ArrayList<HashMap<String, String>> result = OutcomeProcessor.query(MySQL3.getConnection(), checkVoidGames);
            if (result != null) {
                if (!result.isEmpty()) {
                    String gameIDs = result.get(0).get("game_id");
                    if (gameIDs != null) {
                        voidGames = " Game ID " + gameIDs + " was Cancelled";
                    }
                }
            }
        } catch (Exception se) {
            Logging.error(ProcessBets.class.getName() + " Exception thrown", se);
        }
        return voidGames;
    }

    /**
     *
     * @param hashMap
     * @return
     */
    private  OutboxItem msgToSend(HashMap<String, String> hashMap) {
        OutboxItem outboxItem = new OutboxItem();
        try {
            String nextGame = getNextGame();
            String winnerMsg = null;
            if (nextGame != null) {
                winnerMsg = winnerMsg(hashMap.get("possible_win"), hashMap.get("bet_id"), nextGame);
            } else {
                winnerMsg = winnerMsg(hashMap.get("possible_win"), hashMap.get("bet_id"), "");

            }
            String insertToOutbox = insertToOutBox("299011",
                    OutcomeProcessor.getNetwork(hashMap.get("msisdn")), hashMap.get("profile_id"),
                    winnerMsg, hashMap.get("msisdn"));
            String outboxID = OutcomeProcessor.update(MySQL3.getConnection(), insertToOutbox);
            outboxItem.setWinnerMsg(winnerMsg);
            outboxItem.setOutboxID(Integer.parseInt(outboxID));

        } catch (Exception se) {
            Logging.error(ProcessBets.class.getName() + " Exception thrown", se);
        }
        return outboxItem;
    }

    private  String winnerMsg(String possibleWin, String betID,
            String nextGame) {
        String winnerMsg = Props.getWinnerMessage()[0]
                + Props.getWinnerMessage()[1] + " " + betID + " "
                + Props.getWinnerMessage()[2] + " " + possibleWin
                + Props.getWinnerMessage()[3];
        return winnerMsg;
    }

    @Override
    public synchronized String call() throws Exception {
        String betID = null;
        try {
            betID = processWinningBetSlip(matchBets);
        } catch (Exception e) {
            Logging.error("exception thrown: " + e.toString(), e);
            System.err.println("exception thrown: " + e);
        }
        return betID;
    }
}

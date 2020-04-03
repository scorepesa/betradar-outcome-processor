/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import db.MySQL3;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import models.OutcomeItem;
import utils.Constants.OutcomeType;
import utils.Logging;
import utils.Props;

/**
 *
 * @author dennis
 */
public class ProcessOutcomes {

    private transient ExecutorService executor;
    private ProcessMatchResult processMatchResult;

    /**
     * Flag to check if current pool is completed.
     */
    public static boolean isCurrentPoolShutDown = true;

    private long threadStart;

    /**
     *
     * Constructor
     *
     */
    public ProcessOutcomes() {
        executor = Executors.newFixedThreadPool(Props.getNumOfThreads());
    }

    private String getWinOutcomes(OutcomeItem outcome) {
        String winOutcomes = "";
        if (outcome.getOutcomeVoidValue() != null) {
            if (!outcome.getOutcomeVoidValue().isEmpty()) {
                for (HashMap.Entry<String, String> entry : outcome.getOutcomeVoidValue().entrySet()) {
                    winOutcomes = winOutcomes + entry.getValue() + ",";
                }
            }
        }
        if (outcome.getOutcomeValue() != null) {
            winOutcomes = winOutcomes + outcome.getOutcomeValue() + ",";
        }
        winOutcomes = winOutcomes.substring(0, winOutcomes.length() - 1);
        return winOutcomes;
    }

    private String getAllSlipsQuery(String subTypeID, OutcomeItem outcome) {
        String winOutcomes = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();

        //getWinOutcomes(outcome);
        return "SELECT b.bet_id,b.status,p.msisdn,b.profile_id,b.possible_win,b.bet_amount,"
                + " b.refund, b.is_void, b.revised_possible_win, bet_slip_id "
                + " FROM bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN profile p ON b.profile_id = p.profile_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value"
                + " and s.bet_pick = o.winning_outcome )"
                + " WHERE o.is_winning_outcome=1 and "
                + " s.parent_match_id = '" + outcome.getParentMatchID() + "' "
                + " AND s.live_bet = '" + outcome.getLiveBet() + "' "
                + " AND b.status NOT IN (5,7,24, 200) "
                + " AND s.total_games = (SELECT COUNT(IF(status=5 and win=1,1,null)) "
                + " FROM bet_slip WHERE bet_id = b.bet_id)";
    }

    private String updateLoosingBetSlip(OutcomeItem outcome) {
        String winOutcomes = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();
        //getWinOutcomes(outcome);
        return "UPDATE bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value "
                + " and s.bet_pick = o.winning_outcome) "
                + " SET s.bet_slip_id = last_insert_id(bet_slip_id), b.status = 3,"
                + " s.status = 3,b.win = 0,s.win = 0,s.modified = now()  WHERE "
                + " o.is_winning_outcome=0 and "
                + " s.parent_match_id = '" + outcome.getParentMatchID() + "' AND "
                + " s.live_bet =  '" + outcome.getLiveBet() + "' "
                + " AND s.status NOT IN (5,24, 200) AND b.status NOT IN (5,24, 200) ";
    }

    private String updateLoosingOutrightBetSlip(OutcomeItem outcome) {
        String winOutcomes = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();
        //getWinOutcomes(outcome);
        return "UPDATE bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " INNER JOIN outcome o on (o.parent_match_id = s.parent_match_id and "
                + " o.sub_type_id = s.sub_type_id and o.special_bet_value = s.special_bet_value"
                + " and s.bet_pick = o.winning_outcome ) "
                + " SET s.bet_slip_id = last_insert_id(bet_slip_id), s.status = 3,"
                + " s.win = 0,b.status = 3,b.win = 0,s.modified = now()  WHERE "
                + "  o.is_winning_outcome=0 "
                + " s.parent_match_id = '" + outcome.getParentMatchID() + "' AND "
                + " s.live_bet =  '" + outcome.getLiveBet() + "' "
                + " AND o.sub_type_id =  '" + outcome.getSubTypeId() + "' "
                + " AND o.special_bet_value =  '" + outcome.getSpecialBetValue() + "' "
                + " AND o.winning_outcome =  '" + outcome.getWinningOutcome() + "' "
                + " AND s.status NOT IN (5,24, 200)  AND b.status NOT IN (5,24, 200)";
    }

    private String getBetSlipUpdate(OutcomeItem outcome) {
        String betsUpdateQuery = null;
        try {
            if (outcome.getOutcomeType() == OutcomeType.OUTRIGHTOUTCOME) {
                betsUpdateQuery = updateLoosingOutrightBetSlip(outcome);
            } else {
                betsUpdateQuery = updateLoosingBetSlip(outcome);
            }
            String allBetsUpdateQuery = OutcomeProcessor.update(MySQL3.getConnection(), betsUpdateQuery);
            if (allBetsUpdateQuery != null) {
                Logging.info("Updated records " + allBetsUpdateQuery);
                System.out.println("Updated records " + allBetsUpdateQuery);
                return allBetsUpdateQuery;
            } else {
                System.out.println("No Updated records " + allBetsUpdateQuery);
                Logging.info("No Updated records " + allBetsUpdateQuery);
                return betsUpdateQuery;
            }
        } catch (SQLException ex) {
            Logging.error(ProcessOutcomes.class.getName() + " " + ex.getMessage(), ex);
            return betsUpdateQuery;
        }
    }

    private String processWinningBetSlips(String processBetSlips) {
        try {
            String winningBetSlipsUpdateQuery = OutcomeProcessor.update(MySQL3.getConnection(), processBetSlips);
            if (winningBetSlipsUpdateQuery != null) {
                Logging.info("Updated BetSlips records " + winningBetSlipsUpdateQuery);
                System.out.println("Updated BetSlips records " + winningBetSlipsUpdateQuery);
                return winningBetSlipsUpdateQuery;
            } else {
                System.out.println("No Updated records " + winningBetSlipsUpdateQuery);
                Logging.info("No Updated records " + winningBetSlipsUpdateQuery);
                return processBetSlips;
            }
        } catch (SQLException ex) {
            Logging.error(ProcessOutcomes.class.getName() + " " + ex.getMessage(), ex);
            return processBetSlips;
        }
    }

    private String updateWinningBetSlip(String subTypeID, OutcomeItem outcome) {
        String outcomeString = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();
        return "UPDATE bet_slip s inner join outcome o on (s.parent_match_id = "
                + " o.parent_match_id and s.sub_type_id = o.sub_type_id and "
                + " s.special_bet_value  = o.special_bet_value and "
                + " s.bet_pick = o.winning_outcome ) "
                + " SET s.bet_slip_id = last_insert_id(bet_slip_id),"
                + " s.status = 5,s.win = 1,s.modified = now() WHERE "
                + " o.is_winning_outcome =1 and "
                + " s.parent_match_id = '" + outcome.getParentMatchID() + "' "
                + " and s.live_bet = '" + outcome.getLiveBet() + "' "
                + " AND o.sub_type_id =  '" + outcome.getSubTypeId() + "' "
                + " AND o.special_bet_value =  '" + outcome.getSpecialBetValue() + "' "
                + " AND o.winning_outcome =  '" + outcome.getWinningOutcome() + "' "
                + " AND s.status NOT IN (5,24,200)";
    }

    private String updateVoidBet(String voidKey, String voidOutcomes,
            String subTypeID, OutcomeItem outcome) {
        String outcomeString = outcome.getWinningOutcome()
                + ":" + outcome.getSpecialBetValue()
                + ":" + outcome.getSubTypeId();

        Logging.info("Void Bet outcome: Winning outcome =>" + outcome.getWinningOutcome()
                + ", VoidFactor =>" + outcome.getVoidFactor());
        String newWin = " b.total_odd = b.total_odd/(b.total_odd*" + outcome.getVoidFactor() + "), "
                + " b.possible_win = cast(b.possible_win/(s.odd_value*" + outcome.getVoidFactor() + ") as decimal(10, 2)), ";
//                + ""
//                + "((bet_amount - ( "
//                + outcome.getVoidFactor() + " * (b.bet_amount/s.total_games))) "
//                + " *b.total_odd) -((b.bet_amount- ( "
//                + outcome.getVoidFactor() + " * (b.bet_amount/s.total_games)))"
//                + " *b.total_odd*0.18), ";

        return "UPDATE bet_slip s INNER JOIN bet b ON s.bet_id = b.bet_id "
                + " inner join outcome o on (o.parent_match_id = s.parent_match_id "
                + " and o.sub_type_id = s.sub_type_id and "
                + " o.special_bet_value = s.special_bet_value  and "
                + " s.bet_pick = o.winning_outcome ) "
                + " SET s.bet_slip_id = last_insert_id(s.bet_slip_id),"
                + " b.is_void = '" + (outcome.getVoidFactor() > 0 ? "1" : "0") + "', "
                + " s.void_factor = '" + outcome.getVoidFactor() + "', "
                + " b.refund = null, "
                + newWin
                + " s.status = 5,s.win = 1 "
                + " WHERE s.parent_match_id = '" + outcome.getParentMatchID() + "' "
                + " AND s.live_bet = '" + outcome.getLiveBet() + "' "
                + " AND o.sub_type_id =  '" + outcome.getSubTypeId() + "' "
                + " AND o.special_bet_value =  '" + outcome.getSpecialBetValue() + "' "
                + " AND o.winning_outcome =  '" + outcome.getWinningOutcome() + "' "
                + " AND s.status NOT IN (5,24, 200)";
    }

    //Create trx refund the money and log void_bet
    private boolean creditVoidBet(String profileId, String betId, String betSlipId,
            double refundAmount, OutcomeItem outcome) {
        boolean refunded = false;

        Logging.info("Void Bet outcome: Lossing outcome =>" + outcome.getWinningOutcome()
                + ", VoidFactor =>" + outcome.getVoidFactor());

        String trxQ = "insert into transaction (id, profile_id, account,"
                + " iscredit,reference,amount,running_balance, created_by, "
                + " created,modified, status) "
                + " values (null,'" + profileId + "',  '" + profileId + "_VIRTUAL', "
                + " 1, '" + betId + "." + betSlipId + "' , '" + refundAmount + "',  null, 'OutcomeProcessor', "
                + " now(), now(), 'COMPLETE');";

        String vtrx = OutcomeProcessor.update(trxQ);
        if (vtrx != null) {
            String profileQ = "update profile set balance = balance+" + refundAmount + " where "
                    + " profile_id = '" + profileId + "' limit 1";
            OutcomeProcessor.update(profileQ);
            refunded = true;
        }
        return refunded;
    }

    private int processBatchOutcomes(ArrayList<Future<String>> resultSet, OutcomeItem outcome) {
        int counter = 0;
        int status;

        int qSize = resultSet.size();

        while (!resultSet.isEmpty()) {
            Logging.info("Future WinBets : " + resultSet.size());
            for (int i = 0; i < resultSet.size(); i++) {
                Logging.info("Future  : " + resultSet.get(i).isDone());
                try {
                    String betID = null;
                    if (resultSet.get(i) != null) {
                        Logging.info("win Bet" + resultSet.get(i));
                        betID = resultSet.get(i).get();
                        try {
                            Logging.info("Returning win Bet  Bet ID " + resultSet.get(i).get());
                        } catch (InterruptedException | ExecutionException e) {
                            Logging.error("Returning NULL getBetID ", e);
                        }
                    }
                    if (resultSet.get(i).isDone()) {
                        if (betID == null) {
                            Logging.error("Returning NULL win Bet for outcome ID " + outcome.getParentMatchID());
                            resultSet.remove(i);
                        } else {
                            counter++;
                            Logging.info("future.get() is done");
                            resultSet.remove(i);
                        }
                    } else {
                        Logging.error("Future not yet done ..looping " + betID);
                    }
                } catch (InterruptedException | ExecutionException ex) {
                    Logging.error("Future ExecutionException:" + ex.getMessage(), ex);
                } catch (Exception ex) {
                    Logging.error("Future Exception:" + ex.getMessage(), ex);
                }
            }
        }
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - threadStart;
        double elapsedSeconds = tDelta / 1000.0;
        System.err.println("Time taken to execute threads + " + elapsedSeconds);
        Logging.info("Time taken to execute threads + " + elapsedSeconds);
        if (counter != qSize) {
            Logging.info("Some records failed ...");
            System.err.println("Some records failed ...");
            status = -1;
        } else {
            Logging.info("Done all records in batch success ...");
            System.err.println("Done all records in batch success ...");
            status = 1;
        }
        return status;
    }

    public  int processMatchOutcome(OutcomeItem outcome) {
        try {
            Logging.info("Outcome ID not NULL " + outcome.getParentMatchID());
            System.err.println("Outcome ID not NULL " + outcome.getParentMatchID());
            String betSlipsDone = null;
            String oucomeValue;
            if (outcome.getOutcomeValue() == null) {
                Logging.info("Found null outcome trying to reconstuct");
                oucomeValue = outcome.getWinningOutcome()
                        + ":" + outcome.getSpecialBetValue()
                        + ":" + outcome.getSubTypeId();
                Logging.info("Generated new  outcome value => " + oucomeValue);
                outcome.setOutcomeValue(oucomeValue);
            } else {
                //FIX ME: 
                oucomeValue = outcome.getOutcomeValue();
                if (oucomeValue.endsWith(",")) {
                    outcome.setOutcomeValue(
                            oucomeValue.substring(0, oucomeValue.length() - 1));
                }
            }

            if (outcome.getVoidFactor() > 0) {
                //if (!outcome.getOutcomeVoidValue().isEmpty()) {
                System.err.println(" void bet ");
                Logging.info(" void bet ");
                //for (HashMap.Entry<String, String> entry
                //        : outcome.getOutcomeVoidValue().entrySet()) {
                //String key = entry.getKey();
                //String value = entry.getValue();
                String voidBetSlips = null;
                String winOutcomes = outcome.getWinningOutcome()
                        + ":" + outcome.getSpecialBetValue()
                        + ":" + outcome.getSubTypeId();

                if (outcome.getOutcomeType() == OutcomeType.OUTRIGHTOUTCOME) {
                    voidBetSlips = updateVoidBet(
                            String.valueOf(outcome.getVoidFactor()), winOutcomes, "", outcome);
                } else {
                    voidBetSlips = updateVoidBet(String.valueOf(outcome.getVoidFactor()),
                            winOutcomes, ",sub_type_id", outcome);
                }
                if (outcome.isWon()) {
                    Logging.info("Processing winning het for u... WITH DOUBLE CHECK");
                    betSlipsDone = processWinningBetSlips(voidBetSlips);
                }
                // }
                //}
            }
            if (outcome.getOutcomeValue() != null) {
                if (!outcome.getOutcomeValue().isEmpty()) {
                    System.err.println(" normal bet ");
                    Logging.info(" normal bet ");
                    String betSlips = null;
                    if (outcome.getOutcomeType() == OutcomeType.OUTRIGHTOUTCOME) {
                        betSlips = updateWinningBetSlip("", outcome);
                    } else {
                        betSlips = updateWinningBetSlip(",sub_type_id", outcome);
                    }
                    if (outcome.isWon()) {
                        //Just in case
                        betSlipsDone = processWinningBetSlips(betSlips);
                    }
                }
            }
            //only process won betslip
            if (betSlipsDone != null && outcome.isWon()) {
                int status = 0;
                int batchCounter = 0;
                Logging.info("processMatchOutcome called .. ");
                ArrayList<Future<String>> resultSet = new ArrayList<Future<String>>();
                String matchBetsQuery = null;
                if (outcome.getOutcomeType() == OutcomeType.OUTRIGHTOUTCOME) {
                    matchBetsQuery = getAllSlipsQuery("", outcome);
                } else {
                    matchBetsQuery = getAllSlipsQuery(",sub_type_id", outcome);
                }

                Logging.info("Running Query: " + matchBetsQuery);
                boolean allSuccess = true;
                Connection connection = null;
                Statement statement = null;
                ResultSet betSlips = null;
                try {
                    connection = MySQL3.getConnection();
                    statement = connection.createStatement(
                            ResultSet.TYPE_SCROLL_INSENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(Integer.MIN_VALUE);
                    betSlips = statement.executeQuery(matchBetsQuery);
                    threadStart = System.currentTimeMillis();
                    if (betSlips.next()) {
                        System.err.println("Bet_Slips found");
                        betSlips.beforeFirst();
                        System.err.println("Bet_Slips before first");
                        while (betSlips.next()) {
                            batchCounter++;
                            System.err.println("Winning Bets found");
                            try {
                                ResultSetMetaData rsmd = betSlips.getMetaData();
                                HashMap<String, String> matchBets = new HashMap<>();
                                matchBets.put(rsmd.getColumnName(1), betSlips.getString("bet_id"));
                                matchBets.put(rsmd.getColumnName(2), betSlips.getString("status"));
                                matchBets.put(rsmd.getColumnName(3), betSlips.getString("msisdn"));
                                matchBets.put(rsmd.getColumnName(4), betSlips.getString("profile_id"));
                                matchBets.put(rsmd.getColumnName(5), betSlips.getString("possible_win"));
                                matchBets.put(rsmd.getColumnName(6), betSlips.getString("bet_amount"));
                                matchBets.put("revised_possible_win", betSlips.getString("revised_possible_win"));
                                matchBets.put("is_void", betSlips.getString("is_void"));
                                ProcessBets processBets = new ProcessBets(matchBets);
                                Future<String> future = executor.submit(processBets);
                                resultSet.add(future);
                            } catch (SQLException e) {
                                Logging.error("trying to create task ", e);
                            }
                            if (batchCounter % 1000 == 0) {
                                status = processBatchOutcomes(resultSet, outcome);
                                if (status == -1) {
                                    allSuccess = false;
                                }
                            }
                        }
                        if (batchCounter % 1000 != 0) {
                            status = processBatchOutcomes(resultSet, outcome);
                            System.err.println("Status value is " + status);
                            Logging.info("Status value is " + status);
                        }
                        if (!allSuccess) {
                            status = -1;
                        } else {
                            try {
                                processOutcomes(outcome);
                            } catch (Exception e) {
                                Logging.error("Error updating outcomes", e);
                                status = -1;
                            }
                        }
                    } else {
                        try {
                            processOutcomes(outcome);
                        } catch (Exception ex) {
                            Logging.error("SQLException Attempting to process batch update ..", ex);
                            return -1;
                        }
                        status = 1;
                    }
                } catch (SQLException e) {
                    Logging.error(ProcessOutcomes.class.getName() + "  " + e.getMessage(), e);
                    System.err.println(e);
                    System.err.println("Status value is -1");
                    return -1;
                } finally {
                    if (betSlips != null) {
                        try {
                            betSlips.close();
                        } catch (SQLException ex) {
                            Logging.fatal(ProcessOutcomes.class.getName() + "  " + ex.getMessage(), ex);
                        }
                    }
                    if (statement != null) {
                        try {
                            statement.close();
                        } catch (SQLException ex) {
                            Logging.fatal(ProcessOutcomes.class.getName() + "  " + ex.getMessage(), ex);
                        }
                    }
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException ex) {
                            Logging.fatal(ProcessOutcomes.class.getName() + " " + ex.getMessage(), ex);
                        }
                    }
                }
                Logging.info("Done with outcome returning ..." + status);
                return status;
            } else {
                return -1;
            }
        } catch (Exception ex) {
            Logging.fatal(ex.getMessage(), ex);
            try {
                Logging.error("Failed to get Database connection Object rejecting result:" + outcome.getParentMatchID(),
                        ex);
            } catch (Exception e) {
                Logging.error("Failed to reject Request with connection Failure:" + outcome.getParentMatchID(),
                        e);
            }
            return -1;
        } finally {
            Logging.info("Finished Processing All winners ...");
        }
    }

    private void processOutcomes(OutcomeItem outcome) {
        final String winOutcomes = getWinOutcomes(outcome);
        processMatchResult = new ProcessMatchResult(winOutcomes, outcome);
        Thread tConsumer = new Thread(processMatchResult);
        tConsumer.start();
    }

    /**
     * The following method shuts down an ExecutorService in two phases, first
     * by calling shutdown to reject incoming tasks, and then calling
     * shutdownNow, if necessary, to cancel any lingering tasks (after 6
     * minutes).
     *
     * @param pool the executor service pool
     */
    public static void shutdownAndAwaitTermination(final ExecutorService pool, Logging logging) {
        logging.info("Executor pool  waiting for tasks to complete");
        pool.shutdown(); // Disable new tasrks from being submitted

        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                logging.error("Executor pool  terminated with "
                        + "tasks unfinished. Resetting unfinished tasks");
                pool.shutdownNow(); // Cancel currently executing tasks

                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
                    logging.error("Executor pool  terminated "
                            + "with tasks unfinished. Resetting unfinished "
                            + "tasks");

                }
            } else {
                logging.info("Executor pool  completed all "
                        + "tasks and has shut down");
            }
        } catch (InterruptedException ie) {
            logging.error("Executor pool ' shutdown error: "
                    + ie.getMessage());
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Gets whether the current pool has been shut down.
     *
     * @return whether the current pool has been shut down
     */
    public boolean getIsCurrentPoolShutDown() {
        //shutdownAndAwaitTermination(executor,logging);
        return isCurrentPoolShutDown;
    }

}

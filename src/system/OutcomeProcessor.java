package system;

import java.io.IOException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import queue.Consumer;

import utils.Logging;
import utils.Constants;
import utils.Props;

/**
 * This is the Daemon's main class. When loaded, it checks the database for any
 * requests. When a request is found, it queues the request to the job class
 * which sends it to EMG.
 */
@SuppressWarnings("FinalClass")
public final class OutcomeProcessor {

    private transient ExecutorService executor;

    /**
     * System properties class instance.
     */
    public OutcomeProcessor() {
        executor = Executors.newFixedThreadPool(Props.getNumOfConsumers());
    }

    /**
     * Fetch and process message from queue - process 1. Fetch message from
     * queue one by one 2. Find correct routing for each message 3. Create
     * outbound message formatted 4.Send message to defined route Update
     * database with messages state
     */
    private void checkAndProcessMatchWinners() {
        try {
            Logging.info("Creating new consumer instance ... ");
            for (int i = 0; i < Props.getNumOfConsumers(); i++) {
                Consumer msgQueEndPoint = new Consumer(Props.getConsumerQueueNames());
                executor.execute(msgQueEndPoint);
            }
        } catch (IOException ex) {
            System.err.println(" " + ex);
            Logging.error("Exception attempting to create consummer " + ex.getMessage());
        }
    }

    public static ArrayList<HashMap<String, String>> query(Connection connection, String query) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<HashMap<String, String>> results = new ArrayList<>();
        Logging.info("Running Query: " + query);
        //System.err.println("Running Query: " + query);
        try {
            conn = connection;
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            if (rs.next()) {
                Logging.info("ResultSet not null ...");
                ResultSetMetaData metaData = rs.getMetaData();
                String[] columns = new String[metaData.getColumnCount()];
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    columns[i - 1] = metaData.getColumnLabel(i);
                }
                rs.beforeFirst();
                while (rs.next()) {
                    HashMap<String, String> record = new HashMap<String, String>();
                    for (String col : columns) {
                        record.put(col, rs.getString(col));
                    }
                    results.add(record);
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
            return null;
        } catch (Exception ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }
        }
        Logging.info("Found Query Results returning :" + results.size());
        return results;
    }

    public static ArrayList<HashMap<String, String>> query(String query) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<HashMap<String, String>> results = new ArrayList<>();
        Logging.info("Running Query: " + query);
        //System.err.println("Running Query: " + query);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://" + Props.getDbHost()
                    + ":" + Props.getDbPort() + "/" + Props.getDbName(),
                    Props.getDbUserName(),
                    Props.getDbPassword());
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            if (rs.next()) {
                Logging.info("ResultSet not null ...");
                ResultSetMetaData metaData = rs.getMetaData();
                String[] columns = new String[metaData.getColumnCount()];
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    columns[i - 1] = metaData.getColumnLabel(i);
                }
                rs.beforeFirst();
                while (rs.next()) {
                    HashMap<String, String> record = new HashMap<String, String>();
                    for (String col : columns) {
                        record.put(col, rs.getString(col));
                    }
                    results.add(record);
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
            return null;
        } catch (Exception ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }
        }
        Logging.info("Found Query Results returning :" + results.size());
        return results;
    }

    public static String update(Connection connection, String query) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String autoIncKey = null;
        Logging.info("updateBetiko Raw Update Query: " + query);

        try {
            conn = connection;
            stmt = conn.createStatement();
            stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();

            if (rs.next()) {
                autoIncKey = rs.getString(1);
            }
            System.out.println("Auto Increment Key " + autoIncKey);
            rs.close();
            stmt.close();
        } catch (SQLException ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
            OutcomeProcessor.writeToFile(Constants.FAILED_QUERIES_FILE, query, true);
        } catch (Exception ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }
        }
        return autoIncKey;
    }

    public static String update(String query) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String autoIncKey = "";
        Logging.info("updateBetiko Raw Update Query: " + query);

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://" + Props.getDbHost()
                    + ":" + Props.getDbPort() + "/" + Props.getDbName(),
                    Props.getDbUserName(),
                    Props.getDbPassword());
            stmt = conn.createStatement();
            stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();

            if (rs.next()) {
                autoIncKey = rs.getString(1);
            }
            System.out.println("Auto Increment Key " + autoIncKey);
            rs.close();
            stmt.close();
            return autoIncKey;
        } catch (SQLException ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
            OutcomeProcessor.writeToFile(Constants.FAILED_QUERIES_FILE, query, true);
            return null;
        } catch (Exception ex) {
            Logging.error(OutcomeProcessor.class.getName() + "  " + ex.getMessage(), ex);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    Logging.fatal(OutcomeProcessor.class.getName() + " " + ex.getMessage());
                }
            }
        }
    }

    public static String insertToKannelQuery(String sender, String msisdn, String msg,
            String metadata, String smscID) {
        return "insert into kannel.send_sms set sql_id=LAST_INSERT_ID(sql_id),momt='MT',sender='" + sender + "',receiver='" + msisdn + "',"
                + "msgdata='" + msg + "',time=unix_timestamp(),smsc_id='" + smscID + "',"
                + "sms_type=2,boxc_id='sqlbox_content',"
                + "meta_data='" + metadata + "'";
    }

    /**
     *
     * Get a valid prefix number from a number passed as a String argument
     *
     * @param number Phone number passed as String argument
     * @return mobile operator prefix
     */
    public synchronized static String getValidPrefix(String number) {
        String pattern = "(\\+?254|0)?([0-9]{9}$)";
        String prefix = "";
        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);

        // Now create matcher object.
        Matcher m = r.matcher(number);
        // Here is a sample match
        //Found value: m.group(0), 0726986944
        //Found value: m.group(1), 0
        //Found value: m.group(2) 726986944
        if (m.find()) {
            prefix = m.group(1) == null ? "254" : m.group(1);
            prefix = (prefix.equals("0") ? "254" : prefix) + m.group(2).substring(0, 2);
        } else {
            //Well default if you cant match at all
            prefix = "25472";
        }

        return prefix.replace("+", "");

    }

    /**
     *
     * Get the network a phone number uses
     *
     * @param msisdn phone number to get network
     * @return String of network the number uses
     */
    public static String getNetwork(String msisdn) {
        String value = getValidPrefix(String.valueOf(msisdn));
        String network;
        switch (Integer.valueOf(value)) {
            case 25478:
            case 25473:
                network = Constants.AIRTEL;
                break;
            case 25471:
            case 25472:
            case 25474:
            case 25476:
            case 25470:
            case 25479:
                network = Constants.SAFARICOM;
                break;
            case 25477:
                network = Constants.ORANGE;
                break;
            default:
                network = Constants.SAFARICOM;
                break;
        }
        return network;
    }

    /**
     * Processes messages.
     */
    public void processRequests() {
        checkAndProcessMatchWinners();
    }

    public static void writeToFile(String fileName, String text, boolean append) {
        Logging.info("Calling write for file: fileName > " + fileName + ", text=>" + text);
        System.err.println("Calling write for file: fileName > " + fileName + ", text=>" + text);

        try {
            File file = null;
            if (fileName == null) {
                file = new File(Props.getCheckFileStorageDir() + "/" + Constants.FAILED_QUERIES_FILE);
            } else {
                file = new File(Props.getCheckFileStorageDir() + "/" + fileName);
            }

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), append);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(text);
            bw.write("\n");
            bw.close();

        } catch (IOException e) {
            Logging.fatal("Unable to run query : " + text);
            e.printStackTrace();
        }

    }

    /**
     * Gets whether the current pool has been shut down.
     *
     * @return whether the current pool has been shut down
     */
    public boolean getIsCurrentPoolShutDown() {
        return ProcessOutcomes.isCurrentPoolShutDown;
    }

}

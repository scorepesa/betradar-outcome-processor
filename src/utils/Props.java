package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Loads system properties from a file.
 *
 * @author Reuben Paul wafula
 */
@SuppressWarnings({"FinalClass", "ClassWithoutLogger"})
public final class Props {

    /**
     * The properties file.
     */
    private static final String PROPS_FILE = "conf/outcome.processor.conf";
    /**
     * A list of any errors that occurred while loading the properties.
     */
    private List<String> loadErrors;
    /**
     * Info log level. Default = INFO.
     */
    private static String infoLogLevel = "INFO";
    /**
     * Info log level. Default = INFO.
     */
    private static String timerLogLevel = "TIMER";
    /**
     * Error log level. Default = ERROR.
     */
    private static String errorLogLevel = "ERROR";
    /**
     * Fatal log level. Default = FATAL.
     */
    private static String fatalLogLevel = "FATAL";

    private static String checkFileStorageDir = "/var/www/html/";

    /**
     * Error log file name.
     */
    private static String infoLogFile;

    /**
     * Timer log file name.
     */
    private static String timerLogFile;

    /**
     * Error log file name.
     */
    private static String errorLogFile;
    /**
     * Fatal log file name.
     */
    private static String fatalLogFile;

    /**
     * Database user name.
     */
    private static String dbUserName;
    /**
     * Database password.
     */
    private static String dbPassword;
    /**
     * Database host.
     */
    private static String dbHost;
    /**
     * Database port.
     */
    private static String dbPort;
    /**
     * Database name.
     */
    private static String dbName;
    /**
     * Maximum database connections.
     */
    private static int maxConnections;
    /**
     * No of threads that will be created in the thread pool to process
     * payments.
     */
    private transient static int numOfThreads;
    private static double status7 = 0;
    private transient static int numOfConsumers = 0;

    //Rabbit configs
    private static String rabbitHost;
    private static String rabbitPort;
    private static String rabbitUsername;
    private static String rabbitPassword;
    private static String rabbitVhost;

    private static String queueNames;
    private static String consumerQueueNames;

    private static String sdpServiceID;
    private static String safShortCode;
    private static String airtelShortCode;
    private static String orangeShortCode;
    private static String equitelShortCode;

    private static String[] winnerMessage;
    private static String[] jackpotWinnerMessage;
    private static ArrayList<String> twinOutcomeOddTypes;
    private static String userEmails;

    /**
     * Constructor.
     */
    public Props() {
        loadErrors = new ArrayList<String>(0);
        loadProperties(PROPS_FILE);
    }

    /**
     * Load system properties.
     *
     * @param propsFile the system properties xml file
     */
    @SuppressWarnings({"UseOfSystemOutOrSystemErr", "unchecked"})
    private void loadProperties(final String propsFile) {
        FileInputStream propsStream = null;
        Properties props;

        try {
            props = new Properties();
            File base = new File(
                    //System.getProperty("user.dir")
                    Props.class.getProtectionDomain().getCodeSource().getLocation().toURI()
            ).getParentFile();

            propsStream = new FileInputStream(new File(base, propsFile));
            props.load(propsStream);

            String error1 = "ERROR: %s is <= 0 or may not have been set";
            String error2 = "ERROR: %s may not have been set";

            // Extract the values from the configuration file
            setTimerLogLevel(props.getProperty("TimerLogLevel"));
            if (getInfoLogLevel().isEmpty()) {
                loadErrors.add(String.format(error2, "TimerLogLevel"));
            }

            setInfoLogLevel(props.getProperty("InfoLogLevel"));
            if (getInfoLogLevel().isEmpty()) {
                loadErrors.add(String.format(error2, "InfoLogLevel"));
            }

            setErrorLogLevel(props.getProperty("ErrorLogLevel"));
            if (getErrorLogLevel().isEmpty()) {
                loadErrors.add(String.format(error2, "ErrorLogLevel"));
            }

            setFatalLogLevel(props.getProperty("FatalLogLevel"));
            if (getFatalLogLevel().isEmpty()) {
                loadErrors.add(String.format(error2, "FatalLogLevel"));
            }

            setInfoLogFile(props.getProperty("InfoLogFile"));
            if (getInfoLogFile().isEmpty()) {
                loadErrors.add(String.format(error2, "InfoLogFile"));
            }

            setTimerLogFile(props.getProperty("TimerLogFile"));
            if (getTimerLogFile().isEmpty()) {
                loadErrors.add(String.format(error2, "TimerLogFile"));
            }

            setErrorLogFile(props.getProperty("ErrorLogFile"));
            if (getErrorLogFile().isEmpty()) {
                loadErrors.add(String.format(error2, "ErrorLogFile"));
            }

            setFatalLogFile(props.getProperty("FatalLogFile"));
            if (getFatalLogFile().isEmpty()) {
                loadErrors.add(String.format(error2, "FatalLogFile"));
            }

            setCheckFileStorageDir(props.getProperty("FilesDir"));
            if (getCheckFileStorageDir().isEmpty()) {
                loadErrors.add(String.format(error2, "FilesDir"));
            }

            setSafShortCode(props.getProperty("safaricom_message_short_code"));
            if (getSafShortCode().isEmpty()) {
                loadErrors.add(String.format(error2, "safaricom_message_short_code"));
            }

            setAirtelShortCode(props.getProperty("airtel_message_short_code"));
            if (getAirtelShortCode().isEmpty()) {
                loadErrors.add(String.format(error2, "airtel_message_short_code"));
            }

            setOrangeShortCode(props.getProperty("orange_message_short_code"));
            if (getOrangeShortCode().isEmpty()) {
                loadErrors.add(String.format(error2, "orange_message_short_code"));
            }

            setEquitelShortCode(props.getProperty("equitel_message_short_code"));
            if (getEquitelShortCode().isEmpty()) {
                loadErrors.add(String.format(error2, "equitel_message_short_code"));
            }

            setRabbitHost(props.getProperty("RabbitHost"));
            if (getRabbitHost().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitHost"));
            }

            setRabbitPassword(props.getProperty("RabbitPassword"));
            if (getRabbitPassword().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitPassword"));
            }

            setRabbitVhost(props.getProperty("RabbitVhost"));
            if (getRabbitVhost().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitVhost"));
            }

            setRabbitPort(props.getProperty("RabbitPort"));
            if (getRabbitPort().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitPort"));
            }

            setRabbitUsername(props.getProperty("RabbitUsername"));
            if (getRabbitUsername().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitUsername"));
            }

            setDbUserName(props.getProperty("DbUserName"));
            if (getDbUserName().isEmpty()) {
                loadErrors.add(String.format(error2, "DbUserName"));
            }

            setDbPassword(props.getProperty("DbPassword"));
            if (getDbPassword().isEmpty()) {
                loadErrors.add(String.format(error2, "DbPassword"));
            }

            setDbHost(props.getProperty("DbHost"));
            if (getDbHost().isEmpty()) {
                loadErrors.add(String.format(error2, "DbHost"));
            }

            setDbPort(props.getProperty("DbPort"));
            if (getDbPort().isEmpty()) {
                loadErrors.add(String.format(error2, "DbPort"));
            }

            setDbName(props.getProperty("DbName"));
            if (getDbName().isEmpty()) {
                loadErrors.add(String.format(error1, "DbName"));
            }

            String maxConns = props.getProperty("MaximumConnections");
            if (maxConns.isEmpty()) {
                loadErrors.add(String.format(error1,
                        "MaximumConnections"));
            } else {
                setMaxConnections(Integer.parseInt(maxConns));
                if (getMaxConnections() <= 0) {
                    loadErrors.add(String.format(error1,
                            "MaximumConnections"));
                }
            }

            String not = props.getProperty("NumberOfThreads");
            if (not.isEmpty()) {
                loadErrors.add(String.format(error1, "NumberOfThreads"));
            } else {
                setNumOfThreads(Integer.parseInt(not));
                if (getNumOfThreads() <= 0) {
                    loadErrors.add(String.format(error1,
                            "NumberOfThreads"));
                }
            }

            String noc = props.getProperty("NumberOfConsumers");
            if (noc.isEmpty()) {
                loadErrors.add(String.format(error1, "NumberOfConsumers"));
            } else {
                setNumOfConsumers(Integer.parseInt(noc));
                if (getNumOfConsumers() <= 0) {
                    loadErrors.add(String.format(error1,
                            "NumberOfConsumers"));
                }
            }

            setQueueNames(props.getProperty("QueueNames").replaceAll("\\s*,\\s*", ","));
            if (getQueueNames().isEmpty()) {
                loadErrors.add(String.format(error2, "QueueNames"));
            }

            setConsumerQueueNames(props.getProperty("ConsumerQueue2").replaceAll("\\s*,\\s*", ""));
            if (getConsumerQueueNames().isEmpty()) {
                loadErrors.add(String.format(error2, "ConsumerQueue2"));
            }

            setSdpServiceID(props.getProperty("sdp_service_id"));
            if (getSdpServiceID().isEmpty()) {
                loadErrors.add(String.format(error2, "sdp_service_id"));
            }

            String status7 = props.getProperty("status7");
            if (status7.isEmpty()) {
                loadErrors.add(String.format(error1, "status7"));
            } else {
                setStatus7(Double.parseDouble(status7));
                if (getStatus7() <= 0) {
                    loadErrors.add(String.format(error1,
                            "Status 7"));
                }
            }

            String winnerMsg = props.getProperty("winner_msg");
            if (winnerMsg.isEmpty()) {
                loadErrors.add(String.format(error1, "winner_msg"));
            } else {
                //System.err.println("QUEUES @" + queues);
                setWinnerMessage(winnerMsg.split(","));
            }

            String jackpotWinnerMsg = props.getProperty("jackpot_winner_msg");
            if (jackpotWinnerMsg.isEmpty()) {
                loadErrors.add(String.format(error1, "jackpot_winner_msg"));
            } else {
                //System.err.println("QUEUES @" + queues);
                setJackpotWinnerMessage(jackpotWinnerMsg.split(","));
            }

            setUserEmails(props.getProperty("UserEmails"));
            if (getUserEmails().isEmpty()) {
                loadErrors.add(String.format(error2, "UserEmails"));
            }

            String strTwinOutcomeOddTypes = props.getProperty("twinOutcomeOddTypes");
            if (strTwinOutcomeOddTypes.isEmpty()) {
                loadErrors.add(String.format(error1, "twinOutcomeOddTypes"));
            } else {
                //System.err.println("QUEUES @" + queues);
                String[] outcomes = strTwinOutcomeOddTypes.replaceAll("\\s*,\\s*", ",").split(",");
                twinOutcomeOddTypes = new ArrayList<String>(Arrays.asList(outcomes));
                System.err.println("testing" + twinOutcomeOddTypes.get(0));
            }

            propsStream.close();
        } catch (NumberFormatException ne) {
            System.err.println("Exiting. String value found, Integer is "
                    + "required: " + ne.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                        + ex.getMessage());
            }

            System.exit(1);
        } catch (FileNotFoundException | URISyntaxException ne) {
            System.err.println("Exiting. Could not find the properties file: "
                    + ne.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                        + ex.getMessage());
            }

            System.exit(1);
        } catch (IOException ioe) {
            System.err.println("Exiting. Failed to load system properties: "
                    + ioe.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                        + ex.getMessage());
            }

            System.exit(1);
        }
    }

    /**
     * @return the infoLogLevel
     */
    public static String getInfoLogLevel() {
        return infoLogLevel;
    }

    /**
     * @param aInfoLogLevel the infoLogLevel to set
     */
    public static void setInfoLogLevel(String aInfoLogLevel) {
        infoLogLevel = aInfoLogLevel;
    }

    /**
     * @return the timerLogLevel
     */
    public static String getTimerLogLevel() {
        return timerLogLevel;
    }

    /**
     * @param aTimerLogLevel the timerLogLevel to set
     */
    public static void setTimerLogLevel(String aTimerLogLevel) {
        timerLogLevel = aTimerLogLevel;
    }

    /**
     * @return the errorLogLevel
     */
    public static String getErrorLogLevel() {
        return errorLogLevel;
    }

    /**
     * @param aErrorLogLevel the errorLogLevel to set
     */
    public static void setErrorLogLevel(String aErrorLogLevel) {
        errorLogLevel = aErrorLogLevel;
    }

    /**
     * @return the fatalLogLevel
     */
    public static String getFatalLogLevel() {
        return fatalLogLevel;
    }

    /**
     * @param aFatalLogLevel the fatalLogLevel to set
     */
    public static void setFatalLogLevel(String aFatalLogLevel) {
        fatalLogLevel = aFatalLogLevel;
    }

    /**
     * @return the checkFileStorageDir
     */
    public static String getCheckFileStorageDir() {
        return checkFileStorageDir;
    }

    /**
     * @param aCheckFileStorageDir the checkFileStorageDir to set
     */
    public static void setCheckFileStorageDir(String aCheckFileStorageDir) {
        checkFileStorageDir = aCheckFileStorageDir;
    }

    /**
     * @return the infoLogFile
     */
    public static String getInfoLogFile() {
        return infoLogFile;
    }

    /**
     * @param aInfoLogFile the infoLogFile to set
     */
    public static void setInfoLogFile(String aInfoLogFile) {
        infoLogFile = aInfoLogFile;
    }

    /**
     * @return the timerLogFile
     */
    public static String getTimerLogFile() {
        return timerLogFile;
    }

    /**
     * @param aTimerLogFile the timerLogFile to set
     */
    public static void setTimerLogFile(String aTimerLogFile) {
        timerLogFile = aTimerLogFile;
    }

    /**
     * @return the errorLogFile
     */
    public static String getErrorLogFile() {
        return errorLogFile;
    }

    /**
     * @param aErrorLogFile the errorLogFile to set
     */
    public static void setErrorLogFile(String aErrorLogFile) {
        errorLogFile = aErrorLogFile;
    }

    /**
     * @return the fatalLogFile
     */
    public static String getFatalLogFile() {
        return fatalLogFile;
    }

    /**
     * @param aFatalLogFile the fatalLogFile to set
     */
    public static void setFatalLogFile(String aFatalLogFile) {
        fatalLogFile = aFatalLogFile;
    }

    /**
     * @return the dbUserName
     */
    public static String getDbUserName() {
        return dbUserName;
    }

    /**
     * @param aDbUserName the dbUserName to set
     */
    public static void setDbUserName(String aDbUserName) {
        dbUserName = aDbUserName;
    }

    /**
     * @return the dbPassword
     */
    public static String getDbPassword() {
        return dbPassword;
    }

    /**
     * @param aDbPassword the dbPassword to set
     */
    public static void setDbPassword(String aDbPassword) {
        dbPassword = aDbPassword;
    }

    /**
     * @return the dbHost
     */
    public static String getDbHost() {
        return dbHost;
    }

    /**
     * @param aDbHost the dbHost to set
     */
    public static void setDbHost(String aDbHost) {
        dbHost = aDbHost;
    }

    /**
     * @return the dbPort
     */
    public static String getDbPort() {
        return dbPort;
    }

    /**
     * @param aDbPort the dbPort to set
     */
    public static void setDbPort(String aDbPort) {
        dbPort = aDbPort;
    }

    /**
     * @return the dbName
     */
    public static String getDbName() {
        return dbName;
    }

    /**
     * @param aDbName the dbName to set
     */
    public static void setDbName(String aDbName) {
        dbName = aDbName;
    }

    /**
     * @return the maxConnections
     */
    public static int getMaxConnections() {
        return maxConnections;
    }

    /**
     * @param aMaxConnections the maxConnections to set
     */
    public static void setMaxConnections(int aMaxConnections) {
        maxConnections = aMaxConnections;
    }

    /**
     * @return the numOfThreads
     */
    public static int getNumOfThreads() {
        return numOfThreads;
    }

    /**
     * @param aNumOfThreads the numOfThreads to set
     */
    public static void setNumOfThreads(int aNumOfThreads) {
        numOfThreads = aNumOfThreads;
    }

    /**
     * @return the rabbitHost
     */
    public static String getRabbitHost() {
        return rabbitHost;
    }

    /**
     * @param aRabbitHost the rabbitHost to set
     */
    public static void setRabbitHost(String aRabbitHost) {
        rabbitHost = aRabbitHost;
    }

    /**
     * @return the rabbitPort
     */
    public static String getRabbitPort() {
        return rabbitPort;
    }

    /**
     * @param aRabbitPort the rabbitPort to set
     */
    public static void setRabbitPort(String aRabbitPort) {
        rabbitPort = aRabbitPort;
    }

    /**
     * @return the rabbitUsername
     */
    public static String getRabbitUsername() {
        return rabbitUsername;
    }

    /**
     * @param aRabbitUsername the rabbitUsername to set
     */
    public static void setRabbitUsername(String aRabbitUsername) {
        rabbitUsername = aRabbitUsername;
    }

    /**
     * @return the rabbitPassword
     */
    public static String getRabbitPassword() {
        return rabbitPassword;
    }

    /**
     * @param aRabbitPassword the rabbitPassword to set
     */
    public static void setRabbitPassword(String aRabbitPassword) {
        rabbitPassword = aRabbitPassword;
    }

    /**
     * @return the rabbitVhost
     */
    public static String getRabbitVhost() {
        return rabbitVhost;
    }

    /**
     * @param aRabbitVhost the rabbitVhost to set
     */
    public static void setRabbitVhost(String aRabbitVhost) {
        rabbitVhost = aRabbitVhost;
    }

    /**
     * @return the queueNames
     */
    public static String getQueueNames() {
        return queueNames;
    }

    /**
     * @param aQueueNames the queueNames to set
     */
    public static void setQueueNames(String aQueueNames) {
        queueNames = aQueueNames;
    }

    /**
     * @return the consumerQueueNames
     */
    public static String getConsumerQueueNames() {
        return consumerQueueNames;
    }

    /**
     * @param aConsumerQueueNames the consumerQueueNames to set
     */
    public static void setConsumerQueueNames(String aConsumerQueueNames) {
        consumerQueueNames = aConsumerQueueNames;
    }

    /**
     * @return the sdpServiceID
     */
    public static String getSdpServiceID() {
        return sdpServiceID;
    }

    /**
     * @param aSdpServiceID the sdpServiceID to set
     */
    public static void setSdpServiceID(String aSdpServiceID) {
        sdpServiceID = aSdpServiceID;
    }

    /**
     * @return the safShortCode
     */
    public static String getSafShortCode() {
        return safShortCode;
    }

    /**
     * @param aSafShortCode the safShortCode to set
     */
    public static void setSafShortCode(String aSafShortCode) {
        safShortCode = aSafShortCode;
    }

    /**
     * @return the airtelShortCode
     */
    public static String getAirtelShortCode() {
        return airtelShortCode;
    }

    /**
     * @param aAirtelShortCode the airtelShortCode to set
     */
    public static void setAirtelShortCode(String aAirtelShortCode) {
        airtelShortCode = aAirtelShortCode;
    }

    /**
     * @return the orangeShortCode
     */
    public static String getOrangeShortCode() {
        return orangeShortCode;
    }

    /**
     * @param aOrangeShortCode the orangeShortCode to set
     */
    public static void setOrangeShortCode(String aOrangeShortCode) {
        orangeShortCode = aOrangeShortCode;
    }

    /**
     * @return the equitelShortCode
     */
    public static String getEquitelShortCode() {
        return equitelShortCode;
    }

    /**
     * @param aEquitelShortCode the equitelShortCode to set
     */
    public static void setEquitelShortCode(String aEquitelShortCode) {
        equitelShortCode = aEquitelShortCode;
    }

    /**
     * @return the status7
     */
    public static double getStatus7() {
        return status7;
    }

    /**
     * @param aStatus7 the status7 to set
     */
    public static void setStatus7(double aStatus7) {
        status7 = aStatus7;
    }

    /**
     * @return the winnerMessage
     */
    public static String[] getWinnerMessage() {
        return winnerMessage;
    }

    /**
     * @param aWinnerMessage the winnerMessage to set
     */
    public static void setWinnerMessage(String[] aWinnerMessage) {
        winnerMessage = aWinnerMessage;
    }

    /**
     * @return the jackpotWinnerMessage
     */
    public static String[] getJackpotWinnerMessage() {
        return jackpotWinnerMessage;
    }

    /**
     * @param aJackpotWinnerMessage the jackpotWinnerMessage to set
     */
    public static void setJackpotWinnerMessage(String[] aJackpotWinnerMessage) {
        jackpotWinnerMessage = aJackpotWinnerMessage;
    }

    /**
     * @return the userEmails
     */
    public static String getUserEmails() {
        return userEmails;
    }

    /**
     * @param aUserEmails the userEmails to set
     */
    public static void setUserEmails(String aUserEmails) {
        userEmails = aUserEmails;
    }

    /**
     * @return the twinOutcomeOddTypes
     */
    public static ArrayList<String> getTwinOutcomeOddTypes() {
        return twinOutcomeOddTypes;
    }

    /**
     * @param aTwinOutcomeOddTypes the twinOutcomeOddTypes to set
     */
    public static void setTwinOutcomeOddTypes(ArrayList<String> aTwinOutcomeOddTypes) {
        twinOutcomeOddTypes = aTwinOutcomeOddTypes;
    }

    /**
     * @return the numOfConsumers
     */
    public static int getNumOfConsumers() {
        return numOfConsumers;
    }

    /**
     * @param aNumOfConsumers the numOfConsumers to set
     */
    public static void setNumOfConsumers(int aNumOfConsumers) {
        numOfConsumers = aNumOfConsumers;
    }

    /**
     * A list of any errors that occurred while loading the properties.
     *
     * @return the loadErrors
     */
    public List<String> getLoadErrors() {
        return Collections.unmodifiableList(loadErrors);
    }
}

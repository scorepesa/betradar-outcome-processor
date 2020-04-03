/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queue;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import utils.Logging;
import utils.Props;

/**
 *
 * @author karuri
 */
public class QueueConnection {

    private static Connection connection;

    public QueueConnection() {
        try {
            connection = getConnection();
        } catch (Exception e) {
            Logging.error("Exception getRabbitConnection creating connection " + e.getMessage(), e);
        }
    }

    public static boolean checkConnection() {
        //Logging.info("Checking connection");
        return connection != null;
    }

    public static boolean checkConnectionOpen() {
        return connection != null && connection.isOpen();
    }

    /**
     *
     * @return @throws IOException
     */
    public static final Connection getRabbitConnection() throws IOException {
        Logging.info("Attempting to GET NEW connections ... ahu pork came up");
        if (!checkConnection()) {
            ConnectionFactory factory;
            try {
                factory = new com.rabbitmq.client.ConnectionFactory();
                factory.setRequestedHeartbeat(15);
                factory.setConnectionTimeout(5000);
                factory.setAutomaticRecoveryEnabled(true);
                factory.setTopologyRecoveryEnabled(true);

                System.out.println(" Rabbit Host " + Props.getRabbitHost());
                factory.setHost(Props.getRabbitHost());
                factory.setVirtualHost(Props.getRabbitVhost());
                factory.setUsername(Props.getRabbitUsername());
                factory.setPassword(Props.getRabbitPassword());
                factory.setPort(Integer.parseInt(Props.getRabbitPort()));
                factory.setAutomaticRecoveryEnabled(true);
                factory.setRequestedHeartbeat(15);

                // Create a new connection to MQ
                return factory.newConnection();
            } catch (IOException ex) {
                Logging.error("IOException getRabbitConnection creating connection " + ex.getMessage(), ex);
            } catch (TimeoutException ex) {
                Logging.error("TimeoutException getRabbitConnection creating connection " + ex.getMessage(), ex);
            } catch (NumberFormatException ex) {
                Logging.error("Exception getRabbitConnection creating connection " + ex.getMessage(), ex);
            }
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Logging.error(QueueConnection.class.getName() + "Bounced trying to sleep over connection stres",  ex);
        }
        return getRabbitConnection();
    }

    /**
     * Closes the Queue Connection. This is not needed to be called explicitly
     * as connection closure happens implicitly anyways.
     *
     * @throws IOException
     */
    public synchronized static void close() throws IOException {
        try {
            connection.close(); //closing connection, closes all the open channels
            connection = null;
        } catch (AlreadyClosedException e) {
            connection = null;
            Logging.error("Connection Already Closed ", e);
        } catch (IOException e) {
            connection = null;
            Logging.error("Connection Failed to close ", e);
        }
    }

    /**
     * @return the connection
     */
    public synchronized static Connection getConnection() {
        if (connection == null || !connection.isOpen()) {
            try {
                connection = getRabbitConnection();
            } catch (IOException e) {
                Logging.error("Could not reliably get connection ", e);
            }

        }
        return connection;
    }

}

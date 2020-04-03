/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queue;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import utils.Logging;
import utils.Props;

/**
 *
 * @author karuri
 */
public class Publisher extends MessageQueueEndPoint {

    public Publisher() {
        super();
    }

    private synchronized static Channel getChannel(String queue) {
        Channel c = null;
        try {
            //get channel once
            c = getChannel(queue,
                    queue, queue,
                    QueueConnection.getConnection());
            c.basicQos(100);
        } catch (IOException ex) {
            Logging.error(Publisher.class.getName() + " exception getting channel "
                    + ex.getMessage(), ex);
        }
        return c;
    }

    public static void publishMessage(String qMessage, boolean win) throws IOException {
        Logging.info("Attempting to publish message " + win);
        try {
            Channel channel = getChannel(Props.getQueueNames());
            while (channel == null) {
                Logging.error(Publisher.class.getName() + " NULL channel will keep proping for ONE");
                channel = getChannel(Props.getQueueNames());

            }
            Logging.info("Attempting to publish message created channel");
            publish(channel, Props.getQueueNames(), qMessage);
            Logging.info("Attempting to close after publish");

        } catch (Exception e) {
            Logging.error(Publisher.class.getName() + " " + e.getMessage(), e);
        }
    }

    private static void publish(Channel channel, String queue, String qMessage) {
        try {
            long nextSquenceNumber = channel.getNextPublishSeqNo();
            //qMessage.setPublishSequenceNumber(nextSquenceNumber);
            channel.txSelect();
            //System.err.println(" JSON is " + xStream.toXML(qMessage));
            channel.basicPublish(queue, queue,
                    builder.priority(1)
                            .contentType("text/plain")
                            .deliveryMode(2)
                            .build(),
                    qMessage.getBytes());
            channel.txCommit();
        } catch (IOException ex) {
            Logging.error(Publisher.class.getName() + " error publishing message ", ex);
        }
    }

    public static synchronized void publishMessage(String qMessage) throws IOException {
        try {
            Channel channel = getChannel(Props.getQueueNames());
            //channel = getChannel(Props.getConsumerQueueNames(),
            //Props.getConsumerQueueNames(), Props.getConsumerQueueNames(),
            //QueueConnection.getConnection());
            //channel.basicQos(100);

            //long nextSquenceNumber = channel.getNextPublishSeqNo();
            //channel.txSelect();
            System.err.println(" JSON is " + qMessage);
            channel.basicPublish(Props.getConsumerQueueNames(), Props.getConsumerQueueNames(),
                    builder.priority(50).contentType("text/plain").deliveryMode(2).build(),
                    qMessage.getBytes());
            channel.txCommit();

        } catch (IOException e) {
            Logging.error(Publisher.class.getName() + " " + e.getMessage(), e);
        }
    }
}

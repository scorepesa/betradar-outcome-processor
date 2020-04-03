/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queue;

import system.LoosingBets;
import system.ProcessOutcomes;
import system.VoidBets;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import utils.Logging;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import models.OutcomeItem;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import utils.Constants;

/**
 *
 * @author rube
 */
public class Consumer extends MessageQueueEndPoint implements Runnable {

    private final String queueName;
    private final ProcessOutcomes processOutComes;
    private transient Channel channel;
    private transient QueueingConsumer consumer;

    public Consumer(String queueName) throws IOException {
        super();
        Logging.info("Consumer:: Creating consumer ");
        this.queueName = queueName;
        processOutComes = new ProcessOutcomes();
    }

    private QueueingConsumer getConsumer(Channel channel) {
        try {
            consumer = new QueueingConsumer(channel);
            channel.basicQos(100);
            channel.basicConsume(queueName, false, consumer);
            Logging.info(" consumer queue name" + queueName + "" + channel);
            System.err.println(" consumer queue name " + queueName + " " + channel);
            return consumer;
        } catch (IOException ex) {
            Logging.error(Consumer.class.getName() + " " + ex.getMessage(), ex);
            return null;
        }
    }

    public void consume() {
        Logging.info("Calling this.channel.basicConsume ...starting success");
        try {
            channel = getChannel(queueName, queueName, queueName,
                    QueueConnection.getConnection());
            consumer = getConsumer(channel);
        } catch (IOException e) {
            Logging.error("IOException attemting trying acquire channel .." + e.getLocalizedMessage(), e);
            return;
        }

        long tStart = System.currentTimeMillis();
        System.err.println("Time taken to execute start + " + tStart);

        while (true) {
            Logging.info("Waiting for messages ");
            try {
                final QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                Logging.info("Delivery received .... proceeding to process " + Thread.currentThread() + " " + new String(delivery.getBody(), "utf-8"));
                handleDelivery(delivery, channel);
            } catch (ConsumerCancelledException cce) {
                Logging.error("ConsumerCancelledException trying to process message : " + cce.getLocalizedMessage());
                try {
                    channel = getChannel(queueName, queueName, queueName, QueueConnection.getConnection());
                    consumer = getConsumer(channel);
                } catch (IOException e) {
                    Logging.error("IOException attemting trying acquire channel .." + e.getLocalizedMessage(), e);
                    break;
                }
            } catch (InterruptedException ie) {
                Logging.error("InterruptedException trying to process message : " + ie.getLocalizedMessage());
                try {
                    channel = getChannel(queueName, queueName, queueName, QueueConnection.getConnection());
                    consumer = getConsumer(channel);
                } catch (IOException e) {
                    Logging.error("IOException attemting trying acquire channel .." + e.getLocalizedMessage(), e);
                    break;
                }
            } catch (ShutdownSignalException ie) {
                Logging.error("ShutdownSignalException trying to process message : " + ie.getLocalizedMessage());
                try {
                    channel = getChannel(queueName, queueName, queueName, QueueConnection.getConnection());
                    consumer = getConsumer(channel);
                } catch (IOException e) {
                    Logging.error("IOException attemting trying acquire channel .." + e.getLocalizedMessage(), e);
                    break;
                }
            } catch (UnsupportedEncodingException e) {
                Logging.error("ShutdownSignalException trying to process message : " + e.getLocalizedMessage(), e);
            }
            long tEnd = System.currentTimeMillis();
            System.err.println("Time taken to execute end + " + tEnd);
        }
        Logging.info("Broke out of consumption loop will attempt to reconnect ..");
    }

    private void handleDelivery(QueueingConsumer.Delivery delivery, Channel channel) {
        long tStart = System.currentTimeMillis();
        Envelope envelope = delivery.getEnvelope();
        System.err.println("Consumer hadler tag called");
        try {
            String message = new String(delivery.getBody(), "utf-8");
            Logging.info("Received Message:" + message);
            //Logging.info("Creating new task for message received ");
            ArrayList<OutcomeItem> outcomes = parseJsonFromQueue(message);
            if (!outcomes.isEmpty()) {
                try {
                    boolean gotFail = false;
                    int processed_fully = 0;
                    for (OutcomeItem outcome : outcomes) {
                        Logging.info("Processing  outcome ==> "
                                + outcome.isWon() + ", "
                                + outcome.getParentMatchID() + ", "
                                + outcome.getOutcomeSubTypes()
                                + " , " + outcome.getOutcomeValue()
                                + " Void Factor ==>" + outcome.getVoidFactor());

                        if (outcome.getVoidFactor() > 0) {
                            Logging.info("Processing Void outcome void factor ==> "
                                    + outcome.isWon() + ", "
                                    + outcome.getParentMatchID() + ", "
                                    + outcome.getOutcomeSubTypes()
                                    + " , " + outcome.getOutcomeValue()
                                    + " Void Factor ==>" + outcome.getVoidFactor());

                            processVoidBets(outcome);
                        } else if (!outcome.isWon() && outcome.getVoidFactor() == 0.0) {
                            //void factor 1 ... loose refund entire bet
                            Logging.info("Processing loosing outcome with "
                                    + " void factor ==> WON="
                                    + outcome.isWon() + ", "
                                    + outcome.getParentMatchID() + ", "
                                    + outcome.getOutcomeSubTypes()
                                    + " , " + outcome.getOutcomeValue()
                                    + " Void Factor ==>" + outcome.getVoidFactor());
                            Logging.info("Processing void loosing outcome ...");
                            processLoosingBets(outcome);

                        } else if (outcome.isWon()) {
                            Logging.info("Processing winning outcome ==> "
                                    + outcome.isWon() + ", "
                                    + outcome.getParentMatchID() + ", "
                                    + outcome.getOutcomeSubTypes()
                                    + " , " + outcome.getOutcomeValue()
                                    + " Void Factor ==>" + outcome.getVoidFactor());

                            processed_fully = processOutComes.processMatchOutcome(outcome);
                            if (processed_fully == -1) {
                                gotFail = true;
                            }
                            Logging.info("processed fully status = " + processed_fully);
                            // System.err.println("processed fully status = " + processed_fully);
                        }

                        Logging.info("Outcome ID " + outcome.getParentMatchID() + " Processed status " + processed_fully);

                    }
                    if (!gotFail) {
                        Logging.info("Calling message basic ack = processed_fully OK");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        long tEnd = System.currentTimeMillis();
                        long tDelta = tEnd - tStart;
                        double elapsedSeconds = tDelta / 1000.0;
                        System.err.println("Time taken to execute DB transaction  + " + elapsedSeconds);
                        Logging.info("Time taken to execute DB transaction + " + elapsedSeconds);
                    } else {
                        Logging.info("Calling message basic ACK = processed_fully FAILED");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        //channel.basicReject(envelope.getDeliveryTag(), true);
                    }

                } catch (IOException ex) {
                    Logging.error(ex.getMessage());
                    try {
                        Logging.info("Calling message basic reject after IOException");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        //channel.basicReject(envelope.getDeliveryTag(), true);
                    } catch (IOException exception) {
                        Logging.error("Consume message failed basic ack :" + exception.getLocalizedMessage());
                    }

                } catch (Exception e) {
                    Logging.error("Error processing request: " + String.valueOf(outcomes)
                            + "  " + e.getLocalizedMessage(), e);
                    try {
                        Logging.info("Calling message basic reject after Exception");
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        //channel.basicReject(envelope.getDeliveryTag(), true);
                    } catch (IOException exception) {
                        Logging.error("Consume message failed basic Reject :" + exception.getLocalizedMessage(), e);
                    }
                }
            } else {
                Logging.error("Invalid Request from the Queue: " + message);
                try {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                } catch (IOException exception) {
                    Logging.error("Consume message failed basic Reject :" + exception.getLocalizedMessage(), exception);
                }
            }
        } catch (UnsupportedEncodingException ex) {
            Logging.error("Invalid Request from the Queue: ", ex);
            try {
                channel.basicAck(envelope.getDeliveryTag(), false);
            } catch (IOException exception) {
                Logging.error("UnsupportedEncodingException will ACK :" + ex.getLocalizedMessage(), exception);
            }
        }
    }

    private void processVoidBets(OutcomeItem outcome) {
        VoidBets voidBets = new VoidBets(outcome);
        Thread vBets = new Thread(voidBets);
        vBets.start();
    }

    private void processLoosingBets(OutcomeItem outcome) {
        LoosingBets loosingBets = new LoosingBets(outcome);
        Thread tConsumer = new Thread(loosingBets);
        tConsumer.start();
    }

    private  ArrayList<OutcomeItem> parseJsonFromQueue(String message) {
        JSONObject jObject = null;
        OutcomeItem outcomeItem = null;
        ArrayList<OutcomeItem> outs = new ArrayList<>();
        if (message != null) {
            Logging.info("Received outcomes message not null");
            try {
                jObject = new JSONObject(message);
                String parentMatchId = null;
                String liveBet = "0";
                Constants.OutcomeType betType = Constants.OutcomeType.NORMALOUTCOME;

                if (jObject.has("parent_match_id") & !jObject.isNull("parent_match_id")) {
                    Logging.info("NORMAL OUTCOME");
                    parentMatchId = jObject.get("parent_match_id").toString();
                    betType = Constants.OutcomeType.NORMALOUTCOME;
                    liveBet = "0";
                    if(jObject.has("live_bet") ){
                            liveBet = "" + jObject.get("live_bet");
                    }
                    //outs.add(outcomeItem);
                } else if (jObject.has("parent_outright_id") & !jObject.isNull("parent_outright_id")) {
                    Logging.info("OUTRIGHT OUTCOME");
                    parentMatchId = String.valueOf(jObject.get("parent_outright_id"));
                    liveBet = "0";
                    betType = Constants.OutcomeType.OUTRIGHTOUTCOME;
                    //outs.add(outcomeItem);
                } else if (jObject.has("parent_virtual_id") & !jObject.isNull("parent_virtual_id")) {
                    Logging.info("VIRTUAL OUTCOME");
                    parentMatchId = jObject.get("parent_virtual_id").toString();
                    betType = Constants.OutcomeType.VIRTUALOUTCOME;
                    liveBet = "2";
                }
                if (jObject.has("outcomes") & !jObject.isNull("outcomes")) {
                    Logging.info("Found outcomes ");
                    JSONArray jArray = new JSONArray();
                    jArray = jObject.getJSONArray("outcomes");
                    if (parentMatchId == null) {
                        parentMatchId = jObject.get("parent_match_id").toString();
                    }

                    
                    for (int i = 0; i < jArray.length(); i++) {
                        Logging.info("Reading outcome ==" + i);
                        JSONObject jarrObject = jArray.getJSONObject(i);
                        if(jarrObject.has("live_bet")){
                            liveBet = "" + jarrObject.get("live_bet");
                        }
                        HashMap<String, String> hashMap = new HashMap<>();
                        String outcomeValue = "";
                        String outcomeSubTypes = "";
                        outcomeItem = new OutcomeItem();
                        outcomeItem.setLiveBet(liveBet);
                        outcomeItem.setOutcomeType(betType);
                        
                        Logging.info("Reading now ==" + String.valueOf(jarrObject));
                        outcomeItem.setParentMatchID(parentMatchId);
                        Logging.info("Set ParentMatch ID==" + parentMatchId);
                        String winOutcome = "";
                        if (jarrObject.has("outcomeValue") & !jarrObject.isNull("outcomeValue")) {
                            winOutcome = String.valueOf(jarrObject.get("outcomeValue")).replace("'", "''");
                            outcomeItem.setWinningOutcome(winOutcome);
                        }
                        Logging.info("Set Winning outcome ==" + winOutcome);
                        String specialBetValue = "";
                        if (jarrObject.has("specialBetValue") & !jarrObject.isNull("specialBetValue")) {
                            specialBetValue = String.valueOf(jarrObject.get("specialBetValue")).replace("'", "''");
                            outcomeItem.setSpecialBetValue(specialBetValue);
                        }
                        Logging.info("Set Winning specialBetValue ==" + specialBetValue);
                        String subTypeID = "";
                        if (jarrObject.has("odd_type") & !jarrObject.isNull("odd_type")) {
                            subTypeID = String.valueOf(jarrObject.get("odd_type"));
                            outcomeItem.setSubTypeId(subTypeID);
                        }
                        Logging.info("Set Winning subTypeID ==" + subTypeID);

                        outcomeItem.setWon(jarrObject.get("won").equals("1"));

                        Logging.info("Set Winning won ==" + outcomeItem.isWon());
                        if (jarrObject.has("voidFactor")) {
                            outcomeItem.setVoidFactor(Double.valueOf(
                                    jarrObject.get("voidFactor").toString()));
                        } else {
                            outcomeItem.setVoidFactor(0.0);
                        }
                        Logging.info("Set voidFactor won ==" + outcomeItem.getVoidFactor());

                        String voidFactor = "";
                        if (jarrObject.has("voidFactor") & !jarrObject.isNull("voidFactor")) {
                            voidFactor = String.valueOf(jarrObject.get("voidFactor"));
                            if (!voidFactor.equalsIgnoreCase("0.0")) {
                                if (!hashMap.containsKey(voidFactor)) {
                                    String fullOutcome = "'" + winOutcome + ":" + specialBetValue + ":" + subTypeID + "'";
                                    Logging.info(" Void Factor " + voidFactor + " Full Outcome " + fullOutcome);
                                    hashMap.put(voidFactor, fullOutcome);
                                } else {
                                    String fullOutcome = "'" + winOutcome + ":" + specialBetValue + ":" + subTypeID + "'";
                                    String newOutcome = hashMap.get(voidFactor) + "," + fullOutcome;
                                    Logging.info(" Void Factor " + voidFactor + " New Outcome " + newOutcome);
                                    hashMap.put(voidFactor, newOutcome);
                                }
                            } else {
                                String fullOutcome = "'" + winOutcome + ":" + specialBetValue + ":" + subTypeID + "'";
                                outcomeValue = outcomeValue + fullOutcome + ",";
                                outcomeItem.setOutcomeValue(outcomeValue);
                            }
                        } else {
                            String fullOutcome = "'" + winOutcome + ":" + specialBetValue + ":" + subTypeID + "'";
                            outcomeValue = outcomeValue + fullOutcome + ",";
                            outcomeItem.setOutcomeValue(outcomeValue);
                        }
                        outcomeSubTypes = outcomeSubTypes + "'" + subTypeID + "',";

                        if (!hashMap.isEmpty()) {
                            outcomeItem.setOutcomeVoidValue(hashMap);
                            System.err.println(" outcome void value " + hashMap.size());
                            Logging.info(" outcome void value " + hashMap.size());
                        }

                        if (!outcomeSubTypes.equals("")) {
                            outcomeSubTypes = outcomeSubTypes.substring(0, outcomeSubTypes.length() - 1);
                            outcomeItem.setOutcomeSubTypes(outcomeSubTypes);
                            System.err.println(" outcome sub_types " + outcomeSubTypes);
                            Logging.info(" outcome sub_types " + outcomeSubTypes);
                        }

                        outcomeItem.setOutcomeMSG(jObject);
                        outs.add(outcomeItem);
                    }
                }
            } catch (JSONException | NumberFormatException e) {
                outcomeItem = null;
                Logging.error("Error parsing JSON object " + e.getLocalizedMessage(), e);
            }
        }
        Logging.info("Resturing outcomes ==> " + outs.size());
        return outs;
    }

    @Override
    public void run() {
        consume();
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package system;

import java.util.ArrayList;
import java.util.HashMap;
import org.json.JSONArray;
import org.json.JSONObject;
import queue.Publisher;
import utils.Logging;
import utils.Props;

/**
 *
 * @author dennis
 */
public class PublishToQueue {

    /**
     * Logger for this application.
     */
    private static Logging log;
    /**
     * Loads system properties.
     */
    private static Props props;
    /**
     * The main processRequests class.
     */
    private static OutcomeProcessor betiko;

    public static void init() {
        props = new Props();
        log = new Logging();
        log.info("Init starting mysql db connections");

        log.info("Attempting to create RequisitionDaemon");
        System.err.println("Before Fishines init");
        // betRadar = new BetRadar(mysql, log, props);                 
        System.err.println("Fishines init");
    }

    public static void main(final String[] args) {
        init();
        /*  ArrayList<QMessage> listQMessage = new ArrayList<QMessage>();
        QMessage qMessage = new QMessage();
        qMessage.setOutcomeSaveId("48867");
        String strOutComeValue = "2";
        qMessage.setOutcomeValue(strOutComeValue);
        qMessage.setOutcomeKey("1234");
        qMessage.setOddType("10");
        qMessage.setMatchId("7518394");
        listQMessage.add(qMessage);
        QMessage qMessage2 = new QMessage();
        qMessage2.setOutcomeSaveId("21");
        String strOutComeValue2 = "1X";
        qMessage2.setOutcomeValue(strOutComeValue2);
        qMessage2.setOutcomeKey("1233");
        qMessage2.setOddType("46");
        qMessage2.setMatchId("8796220");
        listQMessage.add(qMessage2);
        QMessage qMessage3 = new QMessage();
        qMessage3.setOutcomeSaveId("22");
        String strOutComeValue3 = "12";
        qMessage3.setOutcomeValue(strOutComeValue3);
        qMessage3.setOutcomeKey("1231");
        qMessage3.setOddType("46");
        qMessage3.setMatchId("8796220");
        listQMessage.add(qMessage3);*/
        //publishOutcomeType();                      
        publishOddType();
    }

    private static void publishOutcomeType() {
        ArrayList<String> listQMessage = new ArrayList<String>();
        /*String jsonCreated = createOutcomeJson("0","1234","46","1X","","8796220",21);
        listQMessage.add(jsonCreated);
        String dcJsonCreated = createOutcomeJson("1","1233","46","12","","8796220",22);
        listQMessage.add(dcJsonCreated);*/
        String threeWayJsonCreated = createOutcomeJson("2", "1235", "10", "1", "", "9875670", 1);
        listQMessage.add(threeWayJsonCreated);
        for (int j = 0; j < listQMessage.size(); j++) {
            try {
                Publisher.publishMessage(listQMessage.get(j));
            } catch (Exception ex) {
                log.error("Error publishing message: " + ex);
                System.err.println("Error publishing message: " + ex);
            }
        }
    }

    private static void publishOddType() {
        ArrayList<String> listQMessage = new ArrayList<String>();
        ArrayList<HashMap<String, String>> outcomes = new ArrayList<HashMap<String, String>>();
        /*HashMap<String, String> hashMap = new HashMap();
        hashMap.put("outcomeValue", "Over");
        hashMap.put("specialBetValue", "0.5");
        hashMap.put("liveBet", "0");
        
        HashMap<String, String> hashMap1 = new HashMap();
        hashMap1.put("outcomeValue", "Under");
        hashMap1.put("specialBetValue", "1.5");
        hashMap1.put("liveBet", "0");
        
        HashMap<String, String> hashMap2 = new HashMap();
        hashMap2.put("outcomeValue", "Under");
        hashMap2.put("specialBetValue", "2.5");
        hashMap2.put("liveBet", "0");
        
        HashMap<String, String> hashMap3 = new HashMap();
        hashMap3.put("outcomeValue", "Under");
        hashMap3.put("specialBetValue", "3.5");
        hashMap3.put("liveBet", "0");
        
        HashMap<String, String> hashMap4 = new HashMap();
        hashMap4.put("outcomeValue", "Under");
        hashMap4.put("specialBetValue", "4.5");
        hashMap4.put("liveBet", "0");
        
        HashMap<String, String> hashMap5 = new HashMap();
        hashMap5.put("outcomeValue", "Under");
        hashMap5.put("specialBetValue", "5.5");
        hashMap5.put("liveBet", "0");
        
        outcomes.add(hashMap);
        outcomes.add(hashMap1);
        outcomes.add(hashMap2);
        outcomes.add(hashMap3);
        outcomes.add(hashMap4);
        outcomes.add(hashMap5);
        
        String jsonCreated = createOddTypeJson("0","1234","56",outcomes,"7518394");
        outcomes.removeAll(outcomes);        
        listQMessage.add(jsonCreated);
        
        HashMap<String, String> dcMap = new HashMap();
        dcMap.put("outcomeValue", "12");
        dcMap.put("specialBetValue","");
        dcMap.put("liveBet","0");
        
        HashMap<String, String> dcMap1 = new HashMap();
        dcMap1.put("outcomeValue", "1X");
        dcMap1.put("specialBetValue","");
        dcMap1.put("liveBet","0");
                
        outcomes.add(dcMap);
        outcomes.add(dcMap1);
        
        String dcJsonCreated = createOddTypeJson("1","1233","46",outcomes,"8796220");
        outcomes.removeAll(outcomes);
        listQMessage.add(dcJsonCreated);*/

        HashMap<String, String> threewayMap = new HashMap();
        threewayMap.put("outcomeValue", "1");
        threewayMap.put("specialBetValue", "");
        threewayMap.put("liveBet", "0");

        outcomes.add(threewayMap);

        String threeWayJsonCreated = createOddTypeJson("1", "1236", "10", outcomes, "9769219");
        outcomes.removeAll(outcomes);
        listQMessage.add(threeWayJsonCreated);

        String jsonParentMatchId = "{\n"
                + "	\"parent_match_id\": \"1112522052\",\n"
                + "	\"sequenceNumber\": 0,\n"
                + "	\"outcomes\": [{\n"
                + "		\"outcomeSaveId\": 6,\n"
                + "		\"outcomeKey\": \"3\",\n"
                + "		\"outcomeValue\": \"0:3\",\n"
                + "		\"odd_type\": \"2\"\n"
                + "	}],\n"
                + "	\"live_bet\": 0\n"
                + "}";

        String jsonSingleQuote = "{\n"
                + "	\"parent_match_id\": \"9612615\",\n"
                + "	\"sequenceNumber\": 0,\n"
                + "	\"outcomes\": [{\n"
                + "		\"outcomeSaveId\": 1797603,\n"
                + "		\"outcomeKey\": \"1\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"odd_type\": \"41\"\n"
                + "	}, {\n"
                + "		\"outcomeSaveId\": 1797716,\n"
                + "		\"outcomeValue\": \"Liam Polworth\",\n"
                + "		\"odd_type\": \"235\"\n"
                + "	}, {\n"
                + "		\"outcomeSaveId\": 1797720,\n"
                + "		\"outcomeValue\": \"Alexandre D'Acol Joaquim\",\n"
                + "		\"odd_type\": \"235\"\n"
                + "	}, {\n"
                + "		\"outcomeSaveId\": 1797766,\n"
                + "		\"outcomeKey\": \"1\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"odd_type\": \"390\"\n"
                + "	}],\n"
                + "	\"live_bet\": 0\n"
                + "}";

        String jsonOutright = "{\n"
                + "	\"parent_outright_id\": \"28634\",\n"
                + "	\"outcomes\": [{\n"
                + "		\"outcomeSaveId\": 8,\n"
                + "		\"specialBetValue\": \"\",\n"
                + "		\"outcomeValue\": \"783873\",\n"
                + "		\"voidFactor\": \"0\"\n"
                + "	}]\n"
                + "}";
        String jsonOddTypeID = "{\n"
                + "	\"outcomes\": [{\n"
                + "		\"outcomeKey\": \"1\",\n"
                + "		\"odd_type\": \"1\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"outcomeSaveId\": 1765685,\n"
                + "		\"specialBetValue\": \"1:0\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"40\",\n"
                + "		\"odd_type\": \"2\",\n"
                + "		\"outcomeValue\": \"1:1\",\n"
                + "		\"outcomeSaveId\": 1765687\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"2\",\n"
                + "		\"odd_type\": \"10\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"outcomeSaveId\": 1765689\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"7\",\n"
                + "		\"odd_type\": \"60\",\n"
                + "		\"outcomeValue\": \"Under\",\n"
                + "		\"outcomeSaveId\": 1765691,\n"
                + "		\"specialBetValue\": \"2.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"3\",\n"
                + "		\"odd_type\": \"41\",\n"
                + "		\"outcomeValue\": \"2\",\n"
                + "		\"outcomeSaveId\": 1765693\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"4\",\n"
                + "		\"odd_type\": \"43\",\n"
                + "		\"outcomeValue\": \"Yes\",\n"
                + "		\"outcomeSaveId\": 1765697\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"15\",\n"
                + "		\"odd_type\": \"44\",\n"
                + "		\"outcomeValue\": \"X/X\",\n"
                + "		\"outcomeSaveId\": 1765699\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"69\",\n"
                + "		\"odd_type\": \"46\",\n"
                + "		\"outcomeValue\": \"1X\",\n"
                + "		\"outcomeSaveId\": 1765703\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"71\",\n"
                + "		\"odd_type\": \"46\",\n"
                + "		\"outcomeValue\": \"X2\",\n"
                + "		\"outcomeSaveId\": 1765706\n"
                + "	}, {\n"
                + "		\"voidFactor\": 1,\n"
                + "		\"outcomeKey\": \"1\",\n"
                + "		\"odd_type\": \"47\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"outcomeSaveId\": 1765707\n"
                + "	}, {\n"
                + "		\"voidFactor\": 1,\n"
                + "		\"outcomeKey\": \"3\",\n"
                + "		\"odd_type\": \"47\",\n"
                + "		\"outcomeValue\": \"2\",\n"
                + "		\"outcomeSaveId\": 1765711\n"
                + "	}, {\n"
                + "		\"voidFactor\": 0.5,\n"
                + "		\"outcomeKey\": \"73\",\n"
                + "		\"odd_type\": \"48\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"outcomeSaveId\": 1765714\n"
                + "	}, {\n"
                + "		\"voidFactor\": 0.5,\n"
                + "		\"outcomeKey\": \"73\",\n"
                + "		\"odd_type\": \"49\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"outcomeSaveId\": 1765717\n"
                + "	}, {\n"
                + "		\"voidFactor\": 0.5,\n"
                + "		\"outcomeKey\": \"7\",\n"
                + "		\"odd_type\": \"352\",\n"
                + "		\"outcomeValue\": \"Under\",\n"
                + "		\"outcomeSaveId\": 1765720,\n"
                + "		\"specialBetValue\": \"1.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"6\",\n"
                + "		\"odd_type\": \"353\",\n"
                + "		\"outcomeValue\": \"Over\",\n"
                + "		\"outcomeSaveId\": 1765723,\n"
                + "		\"specialBetValue\": \"0.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"7\",\n"
                + "		\"odd_type\": \"353\",\n"
                + "		\"outcomeValue\": \"Under\",\n"
                + "		\"outcomeSaveId\": 1765726,\n"
                + "		\"specialBetValue\": \"1.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"82\",\n"
                + "		\"odd_type\": \"202\",\n"
                + "		\"outcomeValue\": \"2-3 goals\",\n"
                + "		\"outcomeSaveId\": 1765729\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"3\",\n"
                + "		\"odd_type\": \"55\",\n"
                + "		\"outcomeValue\": \"2\",\n"
                + "		\"outcomeSaveId\": 1765732,\n"
                + "		\"specialBetValue\": \"0:1\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"3\",\n"
                + "		\"odd_type\": \"55\",\n"
                + "		\"outcomeValue\": \"2\",\n"
                + "		\"outcomeSaveId\": 1765736,\n"
                + "		\"specialBetValue\": \"0:2\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"1\",\n"
                + "		\"odd_type\": \"55\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"outcomeSaveId\": 1765739,\n"
                + "		\"specialBetValue\": \"1:0\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"1\",\n"
                + "		\"odd_type\": \"55\",\n"
                + "		\"outcomeValue\": \"1\",\n"
                + "		\"outcomeSaveId\": 1765742,\n"
                + "		\"specialBetValue\": \"2:0\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"6\",\n"
                + "		\"odd_type\": \"56\",\n"
                + "		\"outcomeValue\": \"Over\",\n"
                + "		\"outcomeSaveId\": 1765745,\n"
                + "		\"specialBetValue\": \"0.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"6\",\n"
                + "		\"odd_type\": \"56\",\n"
                + "		\"outcomeValue\": \"Over\",\n"
                + "		\"outcomeSaveId\": 1765749,\n"
                + "		\"specialBetValue\": \"1.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"7\",\n"
                + "		\"odd_type\": \"56\",\n"
                + "		\"outcomeValue\": \"Under\",\n"
                + "		\"outcomeSaveId\": 1765752,\n"
                + "		\"specialBetValue\": \"2.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"7\",\n"
                + "		\"odd_type\": \"56\",\n"
                + "		\"outcomeValue\": \"Under\",\n"
                + "		\"outcomeSaveId\": 1765755,\n"
                + "		\"specialBetValue\": \"3.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"7\",\n"
                + "		\"odd_type\": \"56\",\n"
                + "		\"outcomeValue\": \"Under\",\n"
                + "		\"outcomeSaveId\": 1765758,\n"
                + "		\"specialBetValue\": \"4.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"7\",\n"
                + "		\"odd_type\": \"56\",\n"
                + "		\"outcomeValue\": \"Under\",\n"
                + "		\"outcomeSaveId\": 1765761,\n"
                + "		\"specialBetValue\": \"5.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"106\",\n"
                + "		\"odd_type\": \"208\",\n"
                + "		\"outcomeValue\": \"Over and draw\",\n"
                + "		\"outcomeSaveId\": 1765764,\n"
                + "		\"specialBetValue\": \"1.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"103\",\n"
                + "		\"odd_type\": \"208\",\n"
                + "		\"outcomeValue\": \"Under and draw\",\n"
                + "		\"outcomeSaveId\": 1765767,\n"
                + "		\"specialBetValue\": \"2.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"103\",\n"
                + "		\"odd_type\": \"208\",\n"
                + "		\"outcomeValue\": \"Under and draw\",\n"
                + "		\"outcomeSaveId\": 1765770,\n"
                + "		\"specialBetValue\": \"3.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"103\",\n"
                + "		\"odd_type\": \"208\",\n"
                + "		\"outcomeValue\": \"Under and draw\",\n"
                + "		\"outcomeSaveId\": 1765773,\n"
                + "		\"specialBetValue\": \"4.5\"\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"267\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765776\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"268\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765779\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"223\",\n"
                + "		\"odd_type\": \"270\",\n"
                + "		\"outcomeValue\": \"2+\",\n"
                + "		\"outcomeSaveId\": 1765782\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"315\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765785\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"317\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765787\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"318\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765789\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"320\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765791\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"321\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765793\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"5\",\n"
                + "		\"odd_type\": \"322\",\n"
                + "		\"outcomeValue\": \"No\",\n"
                + "		\"outcomeSaveId\": 1765795\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"69\",\n"
                + "		\"odd_type\": \"323\",\n"
                + "		\"outcomeValue\": \"1X\",\n"
                + "		\"outcomeSaveId\": 1765797\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"71\",\n"
                + "		\"odd_type\": \"323\",\n"
                + "		\"outcomeValue\": \"X2\",\n"
                + "		\"outcomeSaveId\": 1765799\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"4\",\n"
                + "		\"odd_type\": \"328\",\n"
                + "		\"outcomeValue\": \"Yes\",\n"
                + "		\"outcomeSaveId\": 1765801\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"2\",\n"
                + "		\"odd_type\": \"381\",\n"
                + "		\"outcomeValue\": \"X\",\n"
                + "		\"outcomeSaveId\": 1765803\n"
                + "	}, {\n"
                + "		\"outcomeKey\": \"310\",\n"
                + "		\"odd_type\": \"414\",\n"
                + "		\"outcomeValue\": \"Yes / Under\",\n"
                + "		\"outcomeSaveId\": 1765805,\n"
                + "		\"specialBetValue\": \"2.5\"\n"
                + "	}],\n"
                + "	\"parent_match_id\": \"9769219\",\n"
                + "	\"live_bet\": 0,\n"
                + "	\"sequenceNumber\": 0\n"
                + "}";
        String jsonVoid = "{\n"
                + "	\"parent_match_id\": \"9769219\",\n"
                + "	\"sequenceNumber\": 0,\n"
                + "	\"outcomes\": [{\n"
                + "		\"outcomeSaveId\": 1,\n"
                + "		\"outcomeKey\": \"1\",\n"
                + "		\"specialBetValue\": \"\",\n"
                + "		\"outcomeValue\": \"-1\",\n"
                + "		\"odd_type\": \"-1\"\n"
                + "	}],\n"
                + "	\"live_bet\": 0\n"
                + "}";
        String jsonVoido5 = "{\"parent_match_id\":8933153,\"sequenceNumber\":0,\"outcomes\":[{\"outcomeSaveId\":2938983,\"reason\":\"\",\"outcomeKey\":\"\",\"specialBetValue\":\"1.25\",\"outcomeValue\":\"under\",\"voidFactor\":0.5,\"odd_type\":835}],\"live_bet\":1}";

        // for (int j = 0; j < listQMessage.size(); j++) {
        try {
            Publisher.publishMessage(jsonParentMatchId);
        } catch (Exception ex) {
            log.error("Error publishing message: " + ex);
            System.err.println("Error publishing message: " + ex);
        }
        //}        
    }

    public static String createOddTypeJson(String sequenceNumber, String outcomeKey, String oddType, ArrayList<HashMap<String, String>> outcomeValues, String matchID) {
        String jsonCreated = null;
        JSONObject jObject = new JSONObject();
        JSONObject jObject2 = new JSONObject();
        JSONArray jArray = new JSONArray();
        jObject.put("sequenceNumber", sequenceNumber);
        jObject2.put("outcomeKey", outcomeKey);
        jObject2.put("oddType", oddType);
        for (int i = 0; i < outcomeValues.size(); i++) {
            JSONObject jObject3 = new JSONObject();
            jObject3.put("outcomeValue", outcomeValues.get(i).get("outcomeValue"));
            jObject3.put("specialBetValue", outcomeValues.get(i).get("specialBetValue"));
            jObject3.put("liveBet", outcomeValues.get(i).get("liveBet"));
            jArray.put(jObject3);
        }
        jObject2.put("outcome", jArray);
        jObject2.put("matchId", matchID);
        jObject.put("betradar.QMessage", jObject2);
        jsonCreated = jObject.toString();
        System.err.println(jsonCreated);
        return jsonCreated;
    }

    public static String createOutcomeJson(String sequenceNumber, String outcomeKey, String oddType,
            String outcomeValues, String specialBetValue, String matchID, int outcomeSaveId) {
        String jsonCreated = null;
        JSONObject jObject = new JSONObject();
        JSONObject jObject2 = new JSONObject();
        JSONObject jObject3 = new JSONObject();
        JSONArray jArray = new JSONArray();
        jObject.put("sequenceNumber", sequenceNumber);
        jObject2.put("outcomeKey", outcomeKey);
        jObject2.put("oddType", oddType);
        jObject2.put("outcomeSaveId", outcomeSaveId);
        jObject2.put("outcomeValue", outcomeValues);
        jObject2.put("matchId", matchID);
        jObject3.put("specialBetValue", specialBetValue);
        jObject3.put("outcomeValue", outcomeValues);

        jObject2.put("outcome", jObject3);

        jObject.put("betradar.QMessage", jObject2);
        jsonCreated = jObject.toString();
        System.err.println(jsonCreated);
        return jsonCreated;
    }
}

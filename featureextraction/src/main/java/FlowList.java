import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Keshavamurthy on 11/1/16.
 */
public class FlowList {

    private Logger logger = Logger.getLogger(FlowList.class);
    private HashMap<String, HashMap<String, ArrayList<NetFlowData>>> flowsPerScenario;
    private int totalFlows, totalPackets, totalBytes, totalSourceBytes;
    private static DateFormat netFlowDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    private enum ScenarioDurations{
        SCENARIO1_START_DATE("2011/08/10 09:46:53.047277"),
        SCENARIO1_END_DATE("2011/08/10 15:54:07.368340"),
        SCENARIO2_START_DATE("2011/08/11 09:49:35.721274"),
        SCENARIO2_END_DATE("2011/08/11 14:01:11.264754"),
        SCENARIO3_START_DATE("2011/08/12 15:24:01.105063"),
        SCENARIO3_END_DATE("2011/08/15 10:13:26.439802"),
        SCENARIO4_START_DATE("2011/08/15 10:42:52.613616"),
        SCENARIO4_END_DATE("2011/08/15 15:11:19.149682"),
        SCENARIO5_START_DATE("2011/08/15 16:43:20.931208"),
        SCENARIO5_END_DATE("2011/08/15 17:13:26.758894"),
        SCENARIO6_START_DATE("2011/08/16 10:01:46.972101"),
        SCENARIO6_END_DATE("2011/08/16 12:10:56.805092"),
        SCENARIO7_START_DATE("2011/08/16 13:51:24.049047"),
        SCENARIO7_END_DATE("2011/08/16 14:12:41.499668"),
        SCENARIO8_START_DATE("2011/08/16 14:18:55.889839"),
        SCENARIO8_END_DATE("2011/08/17 09:47:11.231725"),
        SCENARIO9_START_DATE("2011/08/17 11:34:49.436881"),
        SCENARIO9_END_DATE("2011/08/17 17:12:13.867435"),
        SCENARIO10_START_DATE("2011/08/18 09:56:29.146156"),
        SCENARIO10_END_DATE("2011/08/18 15:04:59.744388"),
        SCENARIO11_START_DATE("2011/08/18 15:39:35.087798"),
        SCENARIO11_END_DATE("2011/08/18 15:55:46.379941"),
        SCENARIO12_START_DATE("2011/08/19 10:02:43.748728"),
        SCENARIO12_END_DATE("2011/08/19 11:45:43.647861"),
        SCENARIO13_START_DATE("2011/08/15 17:13:40.449530"),
        SCENARIO13_END_DATE("2011/08/16 09:36:00.806547");

        private final String text;

        private ScenarioDurations(String value){
            this.text = value;
        }

        @Override
        public String toString() {
            return this.text;
        }
    }

    private class ExecutionDuration {
        private Date StartDate;
        private Date EndDate;

        ExecutionDuration(String startDate, String endDate) throws ParseException {
            StartDate = FlowList.netFlowDateFormat.parse(startDate);
            EndDate = FlowList.netFlowDateFormat.parse(endDate);
        }

        public boolean IsWithinDuration(String flowStartTime){
            boolean ret = false;
            Date tmpDate = null;
            try {
                tmpDate = FlowList.netFlowDateFormat.parse(flowStartTime);
            } catch (ParseException e) {
                e.printStackTrace();
                return ret;
            }

            if( tmpDate.compareTo(StartDate) >= 0 && tmpDate.compareTo(EndDate) <= 0)
                ret = true;

            return ret;
        }
    }
    
    private class BotnetScenarios{
        private HashMap<String, ExecutionDuration> ScenarioDurationMap = new HashMap<String, ExecutionDuration>();
        BotnetScenarios() throws ParseException {

            for(int scenario = 1, index = 0; scenario <= 13; scenario++, index = index + 2){
                ExecutionDuration scenarioDuration =
                        new ExecutionDuration(ScenarioDurations.values()[index].toString(),
                                ScenarioDurations.values()[index+1].toString());
                String scenarioName = "Scenario" + Integer.toString(scenario);
                ScenarioDurationMap.put(scenarioName, scenarioDuration);
            }

        }

        public String getBotnetScenario(String flowStartTime){

            for (Map.Entry<String, ExecutionDuration> entry:
                 ScenarioDurationMap.entrySet()) {

                if(entry.getValue().IsWithinDuration(flowStartTime))
                    return entry.getKey();
            }

            return "";
        }
    }

    public FlowList(){

        this.flowsPerScenario = new HashMap<String, HashMap<String, ArrayList<NetFlowData>>>();
        this.totalFlows = 0;
        this.totalBytes = 0;
        this.totalPackets = 0;
        this.totalSourceBytes = 0;

    }

    public void AddFlow(NetFlowData flowData) {

        this.totalFlows++;
        this.totalBytes += flowData.getTotalBytes();
        this.totalPackets += flowData.getTotalPackets();
        this.totalSourceBytes += flowData.getTotalSourceBytes();

        BotnetScenarios botnetScenarios = null;
        try {
            botnetScenarios = new BotnetScenarios();
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        String scenarioName = botnetScenarios.getBotnetScenario(flowData.getStartTime());
        HashMap<String, ArrayList<NetFlowData>> flows = null;
        if(this.flowsPerScenario.containsKey(scenarioName)){
            flows = this.flowsPerScenario.get(scenarioName);
        } else {
            flows = new HashMap<String, ArrayList<NetFlowData>>();
            this.flowsPerScenario.put(scenarioName, flows);
        }

        ArrayList<NetFlowData> sourceIPFlows = null;
        String key = Integer.toString(flowData.getDestinationPort());
        if(flows.containsKey( key )){
            sourceIPFlows = flows.get(key);
        } else {
            sourceIPFlows = new ArrayList<NetFlowData>();
            flows.put(key, sourceIPFlows);
        }
        sourceIPFlows.add(flowData);

    }

    public int getTotalFlows() {
        return this.totalFlows;
    }

    private ArrayList<Float> getFlowIAT(ArrayList<NetFlowData> sourceIPFlows) {
        Collections.sort(sourceIPFlows, new Comparator<NetFlowData>() {
            @Override
            public int compare(NetFlowData o1, NetFlowData o2) {
                DateFormat netFlowFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

                Date date1 = null;
                Date date2 = null;
                try {
                    date1 = netFlowFormat.parse(o1.getStartTime());
                    date2 = netFlowFormat.parse(o2.getStartTime());
                } catch (ParseException e){
                    return Integer.parseInt(null);
                }

                return date1.compareTo(date2);
            }
        });

        ArrayList<Float> flowIAT = new ArrayList<Float>();
        Date prevDate = null;
        for (NetFlowData flowData: sourceIPFlows
                ) {
            if(prevDate == null){
                try {
                    prevDate = netFlowDateFormat.parse(flowData.getStartTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }
            }else {
                Date newDate = null;
                try {
                    newDate = netFlowDateFormat.parse(flowData.getStartTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }

                long iat = (newDate.getTime() - prevDate.getTime()) ;
                flowIAT.add( ((Number)iat).floatValue() / 1000 );
            }

        }

        return flowIAT;
    }

    private int getPeriodicCommunicationsPerScenario(HashMap<String, ArrayList<NetFlowData>> flowsPerPort) {
        int totalPeriodicCommunication = 0;

        for (Map.Entry<String, ArrayList<NetFlowData>> entry: flowsPerPort.entrySet()
             ) {
            ArrayList<NetFlowData> sourceIPFlows = entry.getValue();
            if(sourceIPFlows.size() <= 1)
                continue;

            ArrayList<Float> flowsIAT = getFlowIAT(sourceIPFlows);
            if(flowsIAT.size() <= 5)
                continue;

            int totalIAT = 0;
            for (Float flowIAT: flowsIAT
                    ) {
                totalIAT += flowIAT.intValue();
            }

            int meanIAT = totalIAT / sourceIPFlows.size();
            float varianceIAT = 0;
            for (Float flowIAT: flowsIAT
                    ) {
                varianceIAT += (flowIAT.intValue() - meanIAT) * (flowIAT.intValue() - meanIAT);
            }

            varianceIAT = varianceIAT / sourceIPFlows.size();
            float stdDeviationIAT = (float) Math.sqrt(varianceIAT);

            if( stdDeviationIAT <= 2 ){
                totalPeriodicCommunication++;
                logger.info("Periodic Communication Std Deviation: " + Float.toString(stdDeviationIAT) +
                " MEANIAT " + Integer.toString(meanIAT) + " ArrayElements " + flowsIAT.toString() );
            }

        }

        return totalPeriodicCommunication;
    }

    public int getPeriodicCommunications(){
        int totalPeriodicCommunication = 0;
        for (Map.Entry<String, HashMap<String, ArrayList<NetFlowData>>> entry:
             this.flowsPerScenario.entrySet()) {
            totalPeriodicCommunication += getPeriodicCommunicationsPerScenario(entry.getValue());
        }

        return totalPeriodicCommunication;
    }

    public int getTotalPackets() {
        return this.totalPackets;
    }

    public int getTotalBytes(){
        return this.totalBytes;
    }

    public int getTotalSourceBytes(){
        return this.totalSourceBytes;
    }
}

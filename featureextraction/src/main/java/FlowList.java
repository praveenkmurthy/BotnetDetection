import org.apache.hadoop.util.hash.Hash;
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
    private HashMap<String, ArrayList<NetFlowData>> flowsPerPort;
    private int totalFlows, totalPackets, totalBytes, totalSourceBytes;

    public FlowList(){

        this.flowsPerPort = new HashMap<String, ArrayList<NetFlowData>>();
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

        ArrayList<NetFlowData> sourceIPFlows = null;
        String key = Integer.toString(flowData.getSourcePort()) + "-" + Integer.toString(flowData.getDestinationPort());
        if(this.flowsPerPort.containsKey( key )){
            sourceIPFlows = this.flowsPerPort.get(key);
        } else {
            sourceIPFlows = new ArrayList<NetFlowData>();
            this.flowsPerPort.put(key, sourceIPFlows);
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

        DateFormat netFlowFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        ArrayList<Float> flowIAT = new ArrayList<Float>();
        Date prevDate = null;
        for (NetFlowData flowData: sourceIPFlows
                ) {
            if(prevDate == null){
                try {
                    prevDate = netFlowFormat.parse(flowData.getStartTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }
            }else {
                Date newDate = null;
                try {
                    newDate = netFlowFormat.parse(flowData.getStartTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                    continue;
                }

                long iat = (newDate.getTime() - prevDate.getTime()) / 1000;
                flowIAT.add( ((Number)iat).floatValue() );
            }

        }

        return flowIAT;
    }

    public int getPeriodicCommunications() {
        int totalPeriodicCommunication = 0;

        for (Map.Entry<String, ArrayList<NetFlowData>> entry: this.flowsPerPort.entrySet()
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

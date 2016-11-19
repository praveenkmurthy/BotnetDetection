import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Keshavamurthy on 11/1/16.
 */
public class DestinationIPFlowInfo {

    private String destinationIP;
    private Set<NetFlowData.label> outputLabel;
    private HashMap<String, FlowList> sourceIPFlowMap;
    private Set<NetFlowData.proto> differentProtocols;
    private int totalSourceBytes;
    private int totalClientFlows;
    private int totalServerFlows;
    private int totalBidirectionalFlows;

    public DestinationIPFlowInfo(String dstIP){
        this.destinationIP = dstIP;
        this.sourceIPFlowMap = new HashMap<String, FlowList>();
        this.differentProtocols = new HashSet<NetFlowData.proto>();
        this.outputLabel = new HashSet<NetFlowData.label>();
        this.totalSourceBytes = 0;
    }

    private void updateFlowStats(NetFlowData.direction dir){

        if(dir == NetFlowData.direction.BI){
            this.totalBidirectionalFlows++;
        } else if(dir == NetFlowData.direction.CLIENT){
            this.totalClientFlows++;
        } else if(dir == NetFlowData.direction.SERVER) {
            this.totalServerFlows++;
        }
    }

    private void AddProtocol(NetFlowData.proto protocol){
        try {
            this.differentProtocols.add(protocol);
        }
        catch (Exception e){}

    }

    public void AddFlowToMap(NetFlowData flowData) {
        if(this.sourceIPFlowMap.containsKey(flowData.getSourceIP())){
            FlowList flowList = (FlowList) this.sourceIPFlowMap.get(flowData.getSourceIP());
            flowList.AddFlow(flowData);
        } else {
            FlowList flowList = new FlowList();
            flowList.AddFlow(flowData);
            this.sourceIPFlowMap.put(flowData.getSourceIP(), flowList);
        }

        AddProtocol(flowData.getProtocol());
        updateFlowStats(flowData.getDirection());
        setOutputLabel(flowData.getOutputLabel());
        this.totalSourceBytes += flowData.getTotalSourceBytes();

    }

    public int getTotalSourceIPs() {
        return this.sourceIPFlowMap.size();

    }

    public int getTotalProtocols(){
        return this.differentProtocols.size();

    }

    public Set<NetFlowData.proto> getProtocolList() {
        return this.differentProtocols;

    }

    public String getDestinationIP() {
        return destinationIP;
    }

    public String getDestinationIPSubnet() {
        InetAddress destIP;
        try {
            destIP = InetAddress.getByName(destinationIP);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "";
        }

        String subnetString = "";
        int subnet = ByteBuffer.wrap(destIP.getAddress()).getInt() & 0xFFFFFF00;
        try {
            subnetString = InetAddress.getByAddress(ByteBuffer.allocate(4).putInt(subnet).array()).getHostAddress() ;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "";
        }

        return subnetString + "/24";
    }

    public void setDestinationIP(String destinationIP) {
        this.destinationIP = destinationIP;
    }

    public Set<NetFlowData.label> getOutputLabel() {
        return outputLabel;
    }

    public void setOutputLabel(NetFlowData.label outputLabel) {
        try {
            this.outputLabel.add(outputLabel);
        } catch (Exception e) {}

    }

    public int getTotalSourceBytes() {
        return totalSourceBytes;
    }

    public class AggregateInformation {
        private int totalInfo;
        private int maxInfo;
        private int minInfo;
        private float averageInfo;
        private float variance;
        private double standardDeviation;

        public AggregateInformation(){
            this.standardDeviation = 0;
            this.totalInfo = 0;
            this.maxInfo = 0;
            this.minInfo = Integer.MAX_VALUE;
            this.averageInfo = 0;
            this.variance = 0;
        }

        public AggregateInformation(int total, int max, int min, float avg, float variance, double standardDeviation){
            this.totalInfo = total;
            this.maxInfo = max;
            this.minInfo = min;
            this.averageInfo = avg;
            this.variance = variance;
            this.standardDeviation = standardDeviation;
        }

        public int getMaxInfo() {
            return maxInfo;
        }

        public void setMaxInfo(int maxInfo) {
            this.maxInfo = maxInfo;
        }

        public int getMinInfo() {
            return minInfo;
        }

        public void setMinInfo(int minInfo) {
            this.minInfo = minInfo;
        }

        public float getAverageInfo() {
            return averageInfo;
        }

        public void setAverageInfo(float averageInfo) {
            this.averageInfo = averageInfo;
        }

        public float getVariance() {
            return variance;
        }

        public void setVariance(float variance) {
            this.variance = variance;
        }

        public int getTotalInfo() {
            return totalInfo;
        }

        public void setTotalInfo(int totalInfo) {
            this.totalInfo = totalInfo;
        }

        public double getStandardDeviation() {
            return standardDeviation;
        }

        public void setStandardDeviation(double standardDeviation) {
            this.standardDeviation = standardDeviation;
        }
    }

    public AggregateInformation[] getAggregateFeatures() {
        AggregateInformation[] aggregateFeatures = new AggregateInformation[4];

        int maxFlows = 0, minFlows = Integer.MAX_VALUE, totalFlows = 0;
        int maxBytes = 0, minBytes = Integer.MAX_VALUE, totalBytes = 0;
        int maxPackets = 0, minPackets = Integer.MAX_VALUE, totalPackets = 0;
        int maxSourceBytes = 0, minSourceBytes = Integer.MAX_VALUE, totalSourceBytes = 0;

        for (Map.Entry<String, FlowList> entry: this.sourceIPFlowMap.entrySet()
                ) {
            int numberOfBytes = entry.getValue().getTotalBytes();
            if(numberOfBytes > maxBytes)
                maxBytes = numberOfBytes;

            if(numberOfBytes < minBytes)
                minBytes = numberOfBytes;

            totalBytes += numberOfBytes;

            int numberOfPackets = entry.getValue().getTotalPackets();
            if(numberOfPackets > maxPackets)
                maxPackets = numberOfPackets;

            if(numberOfPackets < minPackets)
                minPackets = numberOfPackets;

            totalPackets += numberOfPackets;

            int numberOfFlows = entry.getValue().getTotalFlows();
            if(numberOfFlows > maxFlows)
                maxFlows = numberOfFlows;

            if(numberOfFlows < minFlows)
                minFlows = numberOfFlows;

            totalFlows += numberOfFlows;

            int numberOfSourceBytes = entry.getValue().getTotalSourceBytes();
            if(numberOfSourceBytes > maxSourceBytes)
                maxSourceBytes = numberOfSourceBytes;

            if(numberOfFlows < minSourceBytes)
                minSourceBytes = numberOfSourceBytes;

            totalSourceBytes += numberOfSourceBytes;

        }

        float averageBytes = totalBytes/getTotalSourceIPs();
        float varianceBytes = 0;

        float averageFlows = totalFlows/getTotalSourceIPs();
        float varianceFlows = 0;

        float averagePackets = totalPackets/getTotalSourceIPs();
        float variancePackets = 0;

        float averageSourceBytes = totalSourceBytes/getTotalSourceIPs();
        float varianceSourceBytes = 0;

        for (Map.Entry<String, FlowList> entry: this.sourceIPFlowMap.entrySet()
                ) {
            int bytes = entry.getValue().getTotalBytes();
            varianceBytes += (bytes-averageBytes) * (bytes-averageBytes);

            int flows = entry.getValue().getTotalFlows();
            varianceFlows += (flows-averageFlows) * (flows-averageFlows);

            int packets = entry.getValue().getTotalPackets();
            variancePackets += (packets-averagePackets) * (packets-averagePackets);

            int sourceBytes = entry.getValue().getTotalSourceBytes();
            varianceSourceBytes += (sourceBytes-averageSourceBytes) * (sourceBytes-averageSourceBytes);

        }


        varianceBytes = varianceBytes/getTotalSourceIPs();
        varianceFlows = varianceFlows/getTotalSourceIPs();
        variancePackets = variancePackets/getTotalSourceIPs();
        varianceSourceBytes = varianceSourceBytes/getTotalSourceIPs();

        aggregateFeatures[0] = new AggregateInformation(totalFlows, maxFlows, minFlows, averageFlows, varianceFlows, Math.sqrt(varianceFlows));
        aggregateFeatures[1] = new AggregateInformation(totalPackets, maxPackets, minPackets, averagePackets, variancePackets, Math.sqrt(variancePackets));
        aggregateFeatures[2] = new AggregateInformation(totalBytes, maxBytes, minBytes, averageBytes, varianceBytes, Math.sqrt(varianceBytes));
        aggregateFeatures[3] = new AggregateInformation(totalSourceBytes, maxSourceBytes, minSourceBytes, averageSourceBytes, varianceSourceBytes, Math.sqrt(varianceSourceBytes));

        return aggregateFeatures;
    }

    private int getTotalPeriodicCommunications() {

        int totalPeriodicCommunications = 0;
        for (Map.Entry<String, FlowList> entry: this.sourceIPFlowMap.entrySet()
                ) {
            totalPeriodicCommunications += entry.getValue().getPeriodicCommunications();
        }

        return totalPeriodicCommunications;
    }

    public String getFeatureSet(String subnetFeatures) {
        int protocols = 0;
        for (NetFlowData.proto protocol:
             NetFlowData.proto.values()) {
            if(this.differentProtocols.contains(protocol))
                protocols |= protocol.getValue();
        }

        String aggrFeatures = "";
        AggregateInformation[] aggrFeatureSet = getAggregateFeatures();
        for (AggregateInformation aggregateFeature: aggrFeatureSet
             ) {
            aggrFeatures = String.format("%s %d %d %d %.2f %.2f %.2f", aggrFeatures,
                    aggregateFeature.getTotalInfo(), aggregateFeature.getMaxInfo(), aggregateFeature.getMinInfo(),
                    aggregateFeature.getAverageInfo(), aggregateFeature.getVariance(), aggregateFeature.getStandardDeviation());
        }
        aggrFeatures.trim();
        String featureSet = String.format("%d %d %d %d %d %s %s %s %d %d",
                getTotalSourceIPs(), getTotalProtocols(),
                this.totalBidirectionalFlows, this.totalClientFlows, this.totalServerFlows,
                Integer.toString(protocols), aggrFeatures, subnetFeatures,
                this.getTotalPeriodicCommunications(),
                (this.outputLabel.size() > 1)? 1 : this.outputLabel.iterator().next().getValue()

                );
        return featureSet;

    }
}

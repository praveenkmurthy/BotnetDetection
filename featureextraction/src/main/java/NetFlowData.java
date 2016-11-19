/**
 * Created by Keshavamurthy on 10/25/16.
 */

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.util.NumberTransformer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NetFlowData {

    private static Logger logger = Logger.getLogger(NetFlowData.class);
    private String StartTime;
    private float Duration;
    private proto Protocol;
    private String SourceIP;
    private int SourcePort;
    private direction Direction;
    private String DestinationIP;
    private int DestinationPort;
    private String ConnectionState;
    private int STos;
    private int DTos;
    private int TotalPackets;
    private int TotalBytes;
    private int TotalSourceBytes;
    private label OutputLabel;

    public enum proto {
        NONE(0), TCP(1), UDP(1<<1), ICMP(1<<2), IGMP(1<<3), ARP(1<<4),
        RTP(1<<5), RTCP(1<<6), IPV6ICMP(1<<7), IPXSPX(1<<8),
        UDT(1<<9), UNAS(1<<10), ESP(1<<11), PIM(1<<12), RARP(1<<13),
        IPV6(1<<14), RSVP(1<<15), LLC(1<<16), IPNIP(1<<17), GRE(1<<18);

        private final int value;
        private proto(int value){
            this.value = value;
        }

        public int getValue(){
            return this.value;
        }
    }

    public enum direction {
        CLIENT(0), SERVER(1), BI(2);

        private final int value;
        private direction(int value){
            this.value = value;
        }

        public int getValue(){
            return this.value;
        }
    }

    public enum label {
        NORMAL(0), BOTNET(1), BACKGROUND(2), OTHERFLOW(3);

        private final int value;
        private label(int value){
            this.value = value;
        }

        public int getValue(){
            return this.value;
        }
    }

    public direction getDirection() {
        return Direction;
    }

    public void setDirection(direction direction) {
        Direction = direction;
    }

    public void setDirection(String dir) {
        direction flowDir = direction.BI;
        if(dir.matches("->")) {
            flowDir = direction.CLIENT;
        } else if(dir.matches("<-")) {
            flowDir = direction.SERVER;
        }

        Direction = flowDir;
    }

    public String getStartTime() {
        return StartTime;
    }

    public void setStartTime(String startTime) {
        StartTime = startTime;
    }

    public float getDuration() {
        return Duration;
    }

    public void setDuration(float duration) {
        Duration = duration;
    }

    public proto getProtocol() {
        return Protocol;
    }

    public void setProtocol(proto protocol) {
        Protocol = protocol;
    }

    public void setProtocol(String protocol) {
        protocol = protocol.replaceAll("[/-]", "");
        Protocol = proto.valueOf(protocol);
    }

    public String getSourceIP() {
        return SourceIP;
    }

    public void setSourceIP(String sourceIP) {
        SourceIP = sourceIP;
    }

    public int getSourcePort() {
        return SourcePort;
    }

    public void setSourcePort(int sourcePort) {
        SourcePort = sourcePort;
    }

    public void setSourcePort(String sourcePort) {

        if(!sourcePort.isEmpty()){
            SourcePort = Integer.decode(sourcePort);
        }

    }

    public String getDestinationIP() {
        return DestinationIP;
    }

    public void setDestinationIP(String destinationIP) {
        DestinationIP = destinationIP;
    }

    public String getDestinationIPSubnet() {
        InetAddress destIP;
        try {
            destIP = InetAddress.getByName(DestinationIP);
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

    public int getDestinationPort() {
        return DestinationPort;
    }

    public void setDestinationPort(int destinationPort) {
        DestinationPort = destinationPort;
    }

    public void setDestinationPort(String destinationPort) {

        if(!destinationPort.isEmpty()){
            DestinationPort = Integer.decode(destinationPort);
        }

    }

    public String getConnectionState() {
        return ConnectionState;
    }

    public void setConnectionState(String connectionState) {
        ConnectionState = connectionState;
    }

    public int getSTos() {
        return STos;
    }

    public void setSTos(int STos) {
        this.STos = STos;
    }

    public void setSTos(String STos) {
        if(!STos.isEmpty()){
            this.STos = Integer.decode(STos);
        }
    }

    public int getDTos() {
        return DTos;
    }

    public void setDTos(int DTos) {
        this.DTos = DTos;
    }

    public void setDTos(String DTos) {

        if(!DTos.isEmpty()) {
            this.DTos = Integer.decode(DTos);
        }
    }

    public int getTotalPackets() {
        return TotalPackets;
    }

    public void setTotalPackets(int totalPackets) {
        TotalPackets = totalPackets;
    }

    public int getTotalBytes() {
        return TotalBytes;
    }

    public void setTotalBytes(int totalBytes) {
        TotalBytes = totalBytes;
    }

    public int getTotalSourceBytes() {
        return TotalSourceBytes;
    }

    public void setTotalSourceBytes(int totalSourceBytes) {
        TotalSourceBytes = totalSourceBytes;
    }

    public label getOutputLabel() {
        return OutputLabel;
    }

    public void setOutputLabel(label outputLabel) {
        OutputLabel = outputLabel;
    }

    public void setOutputLabel(String outputLabel) {
        if(outputLabel.toUpperCase().contains("BOTNET")){
            OutputLabel = label.BOTNET;
        } else if(outputLabel.toUpperCase().contains("NORMAL")){
            OutputLabel = label.NORMAL;
        } else if(outputLabel.toUpperCase().contains("BACKGROUND")) {
            OutputLabel = label.BACKGROUND;
        } else
            OutputLabel = label.OTHERFLOW;
    }

    public static boolean ValidateInput(String line, Mapper.Context context) {
        String[] row = line.split(",");

        if(row.length != 15) {
            context.getCounter(FeatureExtraction.CustomCounters.BADINPUT).increment(1);
            return false;
        }

        if(line.contains("StartTime")) {
            context.getCounter(FeatureExtraction.CustomCounters.HEADER).increment(1);
            return false;
        }

        try {
            String protocol = row[2].trim().toUpperCase().replaceAll("[/-]", "");
            proto.valueOf(protocol);
        } catch (IllegalArgumentException e) {
            logger.info("Invalid Protocol " + row[2]);
            context.getCounter(FeatureExtraction.CustomCounters.INVALIDPROTOCOL).increment(1);
            return false;
        }

        Pattern labelPattern = Pattern.compile(".*(normal|botnet).*", Pattern.CASE_INSENSITIVE);
        Matcher labelMatcher = labelPattern.matcher(row[14]);
        if(!labelMatcher.matches()) {
            label outputLabel;
            if (row[14].trim().toUpperCase().contains("BACKGROUND"))
                outputLabel = label.BACKGROUND;
            else
                outputLabel = label.OTHERFLOW;

            context.getCounter(outputLabel).increment(1);
            return false;
        }

        return true;
    }

    public static NetFlowData getInstance(String line, Mapper.Context context) {

        if(!ValidateInput(line, context)){
            return null;
        }

        String[] row = line.split(",");

        NetFlowData flowData = new NetFlowData();
        try {

            flowData.setStartTime(row[0].trim());
            flowData.setDuration(Float.parseFloat(row[1].trim()));
            flowData.setProtocol(row[2].trim().toUpperCase());
            flowData.setSourceIP(row[3].trim());
            flowData.setSourcePort(row[4].trim());
            flowData.setDirection(row[5].trim());
            flowData.setDestinationIP(row[6].trim());
            flowData.setDestinationPort(row[7].trim());
            flowData.setConnectionState(row[8].trim());
            flowData.setSTos(row[9].trim());
            flowData.setDTos(row[10].trim());
            flowData.setTotalPackets(Integer.parseInt(row[11].trim()));
            flowData.setTotalBytes(Integer.parseInt(row[12].trim()));
            flowData.setTotalSourceBytes(Integer.parseInt(row[13].trim()));
            flowData.setOutputLabel(row[14].trim());

        }
        catch (Exception e){
            context.getCounter(FeatureExtraction.CustomCounters.BADINPUT).increment(1);
            logger.info("Exception while reading the line -- " + line + " -- " + e.getMessage() + " " + Thread.currentThread().getStackTrace().toString());
            logger.error("Stack Trace", e);
            return null;
        }


        return  flowData;
    }
}

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Keshavamurthy on 11/1/16.
 */
public class NetFlowFormat implements Writable {

    private Text StartTime;
    private FloatWritable Duration;
    private IntWritable Protocol;
    private Text SourceIP;
    private IntWritable SourcePort;
    private IntWritable Direction;
    private Text DestinationIP;
    private IntWritable DestinationPort;
    private Text ConnectionState;
    private IntWritable STos;
    private IntWritable DTos;
    private IntWritable TotalPackets;
    private IntWritable TotalBytes;
    private IntWritable TotalSourceBytes;
    private IntWritable OutputLabel;

    public NetFlowFormat() {
        this.StartTime = new Text();
        this.Duration = new FloatWritable();
        this.Protocol = new IntWritable();
        this.SourceIP = new Text();
        this.SourcePort = new IntWritable();
        this.Direction = new IntWritable();
        this.DestinationIP = new Text();
        this.DestinationPort = new IntWritable();
        this.ConnectionState = new Text();
        this.STos = new IntWritable();
        this.DTos = new IntWritable();
        this.TotalPackets = new IntWritable();
        this.TotalBytes = new IntWritable();
        this.TotalSourceBytes = new IntWritable();
        this.OutputLabel = new IntWritable();

    }

    public NetFlowFormat(NetFlowData netFlowInfo) {
        this.StartTime = new Text(netFlowInfo.getStartTime());
        this.Duration = new FloatWritable(netFlowInfo.getDuration());
        this.Protocol = new IntWritable(netFlowInfo.getProtocol().getValue());
        this.SourceIP = new Text(netFlowInfo.getSourceIP());
        this.SourcePort = new IntWritable(netFlowInfo.getSourcePort());
        this.Direction = new IntWritable(netFlowInfo.getDirection().getValue());
        this.DestinationIP = new Text(netFlowInfo.getDestinationIP());
        this.DestinationPort = new IntWritable(netFlowInfo.getDestinationPort());
        this.ConnectionState = new Text(netFlowInfo.getConnectionState());
        this.STos = new IntWritable(netFlowInfo.getSTos());
        this.DTos = new IntWritable(netFlowInfo.getDTos());
        this.TotalPackets = new IntWritable(netFlowInfo.getTotalPackets());
        this.TotalBytes = new IntWritable(netFlowInfo.getTotalBytes());
        this.TotalSourceBytes = new IntWritable(netFlowInfo.getTotalSourceBytes());
        this.OutputLabel = new IntWritable(netFlowInfo.getOutputLabel().getValue());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.StartTime.write(dataOutput);
        this.Duration.write(dataOutput);
        this.Protocol.write(dataOutput);
        this.SourceIP.write(dataOutput);
        this.SourcePort.write(dataOutput);
        this.Direction.write(dataOutput);
        this.DestinationIP.write(dataOutput);
        this.DestinationPort.write(dataOutput);
        this.ConnectionState.write(dataOutput);
        this.STos.write(dataOutput);
        this.DTos.write(dataOutput);
        this.TotalPackets.write(dataOutput);
        this.TotalBytes.write(dataOutput);
        this.TotalSourceBytes.write(dataOutput);
        this.OutputLabel.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.StartTime.readFields(dataInput);
        this.Duration.readFields(dataInput);
        this.Protocol.readFields(dataInput);
        this.SourceIP.readFields(dataInput);
        this.SourcePort.readFields(dataInput);
        this.Direction.readFields(dataInput);
        this.DestinationIP.readFields(dataInput);
        this.DestinationPort.readFields(dataInput);
        this.ConnectionState.readFields(dataInput);
        this.STos.readFields(dataInput);
        this.DTos.readFields(dataInput);
        this.TotalPackets.readFields(dataInput);
        this.TotalBytes.readFields(dataInput);
        this.TotalSourceBytes.readFields(dataInput);
        this.OutputLabel.readFields(dataInput);

    }

    @Override
    public String toString(){
        String retStr = String.format("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s",
                this.StartTime.toString(), this.Duration.toString(), this.Protocol.toString(), this.SourceIP.toString(),
                this.SourcePort.toString(), this.Direction.toString(), this.DestinationIP.toString(), this.DestinationPort.toString(),
                this.ConnectionState.toString(), this.STos.toString(), this.DTos.toString(), this.TotalPackets.toString(), this.TotalBytes.toString(),
                this.TotalSourceBytes.toString(), this.OutputLabel.toString());

        return retStr;
    }

    private NetFlowData.proto getProtocolType(){

        NetFlowData.proto protocol = NetFlowData.proto.NONE;
        if(this.Protocol.get() == NetFlowData.proto.TCP.getValue()){
            protocol = NetFlowData.proto.TCP;
        } else if(this.Protocol.get() == NetFlowData.proto.UDP.getValue()){
            protocol = NetFlowData.proto.UDP;
        } else if(this.Protocol.get() == NetFlowData.proto.ICMP.getValue()){
            protocol = NetFlowData.proto.ICMP;
        } else if(this.Protocol.get() == NetFlowData.proto.IGMP.getValue()){
            protocol = NetFlowData.proto.IGMP;
        } else if(this.Protocol.get() == NetFlowData.proto.ARP.getValue()){
            protocol = NetFlowData.proto.ARP;
        } else if(this.Protocol.get() == NetFlowData.proto.RTP.getValue()){
            protocol = NetFlowData.proto.RTP;
        }

        return protocol;
    }

    public NetFlowData toNetFlowData(){
        NetFlowData netFlowInfo = new NetFlowData();
        netFlowInfo.setStartTime(this.StartTime.toString());
        netFlowInfo.setDuration(this.Duration.get());
        netFlowInfo.setProtocol(getProtocolType());
        netFlowInfo.setSourceIP(this.SourceIP.toString());
        netFlowInfo.setSourcePort(this.SourcePort.get());
        netFlowInfo.setDirection(NetFlowData.direction.values()[this.Direction.get()]);
        netFlowInfo.setDestinationIP(this.DestinationIP.toString());
        netFlowInfo.setDestinationPort(this.DestinationPort.get());
        netFlowInfo.setConnectionState(this.ConnectionState.toString());
        netFlowInfo.setSTos(this.STos.get());
        netFlowInfo.setDTos(this.DTos.get());
        netFlowInfo.setTotalPackets(this.TotalPackets.get());
        netFlowInfo.setTotalBytes(this.TotalBytes.get());
        netFlowInfo.setTotalSourceBytes(this.TotalSourceBytes.get());
        netFlowInfo.setOutputLabel(NetFlowData.label.values()[this.OutputLabel.get()]);

        return netFlowInfo;
    }
}

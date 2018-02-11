import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;

public class HintedhandoffHandler implements Runnable {
	private int port, thisPort;
	private String ip;
	private Map<String, Map<Integer, ValueTuple>> hintMap;

	public HintedhandoffHandler(Map<String, Map<Integer, ValueTuple>> hintMap, String ip, int port, int thisPort) {
		this.ip = ip;
		this.port = port;
		this.thisPort = thisPort;
		this.hintMap = hintMap;
	}

	@Override
	public void run() {
		try{
		Server server = Server.getInstance();
		String ipPort = ip+" "+port;
		Map<Integer, ValueTuple> keyValue = hintMap.get(ipPort);
		for(int key: keyValue.keySet()){
			ValueTuple vt = keyValue.get(key);
			Casandra.NodeMessage.Builder nm = Casandra.NodeMessage.newBuilder();
			Casandra.HintMessage.Builder hm = Casandra.HintMessage.newBuilder();
			hm.setFrom(InetAddress.getLocalHost().getHostAddress()+" "+thisPort);
			hm.setKey(key);
			hm.setValue(vt.getValue());
			hm.setTimestamp(vt.getTimeStamp());
			nm.setHintMessage(hm.build());
			Socket client = new Socket(ip,port);
			nm.build().writeDelimitedTo(client.getOutputStream());
			Casandra.NodeMessage ackMessage = Casandra.NodeMessage.parseDelimitedFrom(client.getInputStream());
			client.close();
			if(ackMessage.hasAckWrite()){
				keyValue.remove(key);
				server.writeHint("delete "+ipPort+" "+vt.getTimeStamp()+" "+key+" "+vt.getValue()+"\n");
			}
		}
		}catch(IOException e){
			
		}
		
	}

}

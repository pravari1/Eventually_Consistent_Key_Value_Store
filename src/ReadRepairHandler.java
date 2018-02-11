import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;


public class ReadRepairHandler implements Runnable {
	public final int key;
	ReadRepairHandler(int keyArg){
		key = keyArg;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		Server server = null;
		try {
			server = Server.getInstance();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		if(server == null) return;
		
		String currip = null;
		try {
			currip = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		}
		int currport = Server.getPORT();
		String fromAddr = currip +" "+currport;
				
		int index = server.getPrimaryNodeIndex(key); // primary replica index
		List<Casandra.ReturnVal> valueTuples = new ArrayList<Casandra.ReturnVal>();
		int latest = 0;
		long max_timestamp = 0;
		for(int i=0;i<4;i++) {
			String node = server.getAllNodeList().get(index);
			String ip = node.split(" ")[0];
			int port = Integer.parseInt(node.split(" ")[1]);
			Casandra.ReadFromReplica.Builder readFromReplica = Casandra.ReadFromReplica.newBuilder();
			readFromReplica.setKey(key);
			Casandra.NodeMessage.Builder nodeMessage = Casandra.NodeMessage.newBuilder();
			nodeMessage.setReadFromReplica(readFromReplica.build());
			
			try {
				Socket client = new Socket(ip,port);
				nodeMessage.build().writeDelimitedTo(client.getOutputStream());
				Casandra.NodeMessage retMsg = Casandra.NodeMessage.parseDelimitedFrom(client.getInputStream());
				Casandra.ReturnVal retVal = null;
				if(retMsg.hasReturnVal()){
					retVal = retMsg.getReturnVal();
				}else{
					Casandra.ReturnVal.Builder retValBuilder = Casandra.ReturnVal.newBuilder();
					retValBuilder.setFrom(ip+" "+port);
					retValBuilder.setTimestamp(0);
					retValBuilder.setKey(key);
					retVal = retValBuilder.build();
				}
				valueTuples.add(retVal);
				if(retVal.getTimestamp() > max_timestamp) {
					latest = valueTuples.size()-1;
					max_timestamp = retVal.getTimestamp();
				}
				client.close();
			} catch (UnknownHostException e) {
			} catch (IOException e) {
			}
			index++;
			if(index == server.getAllNodeList().size())
				index = 0;
		}
		
		// find the latest value by timestamp
		index = server.getPrimaryNodeIndex(key);
		for(int i=0;i<valueTuples.size();i++) {
			if(max_timestamp == valueTuples.get(i).getTimestamp()) { // have same value
				continue;
			}
			String node = valueTuples.get(i).getFrom();
			String ip = node.split(" ")[0];
			int port = Integer.parseInt(node.split(" ")[1]);
			Casandra.PutReplicaServer.Builder putMsg = Casandra.PutReplicaServer.newBuilder();
			putMsg.setKey(key).setTimestamp(max_timestamp).setValue(valueTuples.get(latest).getValue()).setFrom(fromAddr);
			
			Casandra.NodeMessage.Builder nodeMessage = Casandra.NodeMessage.newBuilder();
			nodeMessage.setPutReplicaServer(putMsg.build());
			
			try {
				Socket client = new Socket(ip,port);
				nodeMessage.build().writeDelimitedTo(client.getOutputStream());
				
				client.close();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
	}

}

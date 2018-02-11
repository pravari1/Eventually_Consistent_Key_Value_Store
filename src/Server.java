import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ValueTuple{
	private String value;
	private long timeStamp;
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public ValueTuple(String val, long time) {
		// TODO Auto-generated constructor stub
		value = val;
		timeStamp = time;
	}
}

public class Server {
	
	private static int PORT;
	private static String mode = "rr";
	private List<String> allNodeList;
	private Map<Integer, ValueTuple> keyValStore = new HashMap<>();
	private Map<String, Map<Integer,ValueTuple>> hintMap = new HashMap<String, Map<Integer,ValueTuple>>();

	public static int getPORT() {
		return PORT;
	}

	public static void setPORT(int pORT) {
		PORT = pORT;
	}

	public List<String> getAllNodeList() {
		return allNodeList;
	}

	public void setAllNodeList(List<String> allNodeList) {
		this.allNodeList = allNodeList;
	}

	public Map<Integer, ValueTuple> getKeyValStore() {
		return keyValStore;
	}

	public void setKeyValStore(Map<Integer, ValueTuple> keyValStore) {
		this.keyValStore = keyValStore;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if(args.length != 3) {
			System.out.println("Enter <PORT> <mode> <nodes_file_path>");
			System.exit(1);
		}
		PORT = Integer.parseInt(args[0]);
		mode = args[1];
		Server server = Server.getInstance();

		ServerSocket socket = null;
		try {
			server.initialize(args[2]);
			socket = new ServerSocket(PORT);
			Socket client = null;
			server.replayLogs();
			if(server.mode.equals("hh")) {
				server.replayLogsHints();
			}
			while (true) {
				client = socket.accept();
				InputStream inputStream = client.getInputStream();
				Casandra.NodeMessage nodeMessage = Casandra.NodeMessage.parseDelimitedFrom(inputStream);
				if(nodeMessage == null) {
					client.close();
					continue;
				}
				if(nodeMessage.hasReadValue()) { //Read request from client
					server.fetchValueFromAllReplica(nodeMessage,client);	
				}
				else if(nodeMessage.hasReadFromReplica()) { //Read request from coordinator
					server.getValueForKey(nodeMessage,client);
				}else if(nodeMessage.hasReturnVal()) {
					server.processReadReturnValue(nodeMessage,client);
				}else if(nodeMessage.hasPut()) {
					server.sendValueToAllReplica(nodeMessage, client);
				} else if (nodeMessage.hasPutReplicaServer()) {
					server.writeValue(nodeMessage.getPutReplicaServer().getFrom(),nodeMessage.getPutReplicaServer().getKey(), nodeMessage.getPutReplicaServer().getValue(), nodeMessage.getPutReplicaServer().getTimestamp(), client);
				} else if (nodeMessage.hasReadRepair()) {
					server.writeValue(nodeMessage.getReadRepair().getFrom(), nodeMessage.getReadRepair().getKey(), nodeMessage.getReadRepair().getValue(), nodeMessage.getReadRepair().getTimestamp(), client);
				}else if(nodeMessage.hasHintMessage()) {
					Casandra.HintMessage hintMsg = nodeMessage.getHintMessage(); 
					if(server.getKeyValStore().get(hintMsg.getKey()) == null || server.getKeyValStore().get(hintMsg.getKey()).getTimeStamp() < hintMsg.getTimestamp()) {
						server.writeValue(nodeMessage.getHintMessage().getFrom(),nodeMessage.getHintMessage().getKey(), nodeMessage.getHintMessage().getValue(), nodeMessage.getHintMessage().getTimestamp(), client);
					}else {
						Casandra.AckWrite.Builder ackWrite = Casandra.AckWrite.newBuilder();
						ackWrite.setFrom(InetAddress.getLocalHost().getHostAddress() + " "+ PORT);
						Casandra.NodeMessage  nodeMsg = Casandra.NodeMessage.newBuilder().setAckWrite(ackWrite).build();
						
						nodeMsg.writeDelimitedTo(client.getOutputStream());
					}
					
				}
				client.close();
			}
		} catch (IOException e) {
			// TODO: handle exception
			//e.printStackTrace();
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					//e1.printStackTrace();
				}
			}
		}
	}

	private void processReadReturnValue(Casandra.NodeMessage nodeMessage,
			Socket client) {
		// TODO Auto-generated method stub

	}
	
	public void replayLogs() throws IOException{
		
		File writeLogFile = new File(InetAddress.getLocalHost().getHostAddress()+"_"+PORT+"_"+"WriteLog.txt");
		
		if(!writeLogFile.exists()) {
			return;
		}
		BufferedReader br = new BufferedReader(new FileReader(InetAddress.getLocalHost().getHostAddress()+"_"+PORT+"_"+"WriteLog.txt"));
		String line="";
		while((line=br.readLine())!=null){
			String input[] = line.split(" ");
			String value = input[2];
			for(int i=3;i<input.length;i++) {
				value = value + " " + input[i];
			}
			ValueTuple vt = new ValueTuple(value, Long.parseLong(input[0]));
			keyValStore.put(Integer.parseInt(input[1]), vt);
		}
		br.close();
	}
	
	public void replayLogsHints() throws IOException {
		
		File hintFile = new File(InetAddress.getLocalHost().getHostAddress()+"_"+PORT+"_"+"hints.txt");
		if(!hintFile.exists()) {
			return;
		}
		BufferedReader br = new BufferedReader(new FileReader(InetAddress.getLocalHost().getHostAddress()+"_"+PORT+"_"+"hints.txt"));
		String line="";
		hintMap = new HashMap<>();
		while((line=br.readLine())!=null){
			String input[] = line.split(" ");
			boolean isDelete = false;
			String ipPort = "";
			Integer key = 0;
			String value = "";
			long timestamp = 0;
			if(input[0].equals("delete")) {
				isDelete = true;
				ipPort = input[1]+" "+input[2];
				key = Integer.parseInt(input[4]);
			}else {
				ipPort = input[0]+" "+input[1];
				timestamp = Long.parseLong(input[2]);
				key = Integer.parseInt(input[3]);
				value = input[4];
				for(int i=5;i<input.length;i++) {
					value = value + " " + input[i];
				}
//				value = value.substring(0,value.length()-1);
			}
			if(hintMap.get(ipPort) == null) {
				Map<Integer, ValueTuple> map = new HashMap<>();
				map.put(key, new ValueTuple(value, timestamp));
				hintMap.put(ipPort,map);
			}else {
				if(isDelete) {
					hintMap.get(ipPort).remove(key);
				}else {
					hintMap.get(ipPort).put(key, new ValueTuple(value, timestamp));
				}
			}
		}
		br.close();
	}

	private void getValueForKey(Casandra.NodeMessage nodeMessage, Socket client)
			throws IOException {

		Casandra.ReadFromReplica rReplica = nodeMessage.getReadFromReplica();
		int key = rReplica.getKey();
		ValueTuple val = getKeyValStore().get(key);
		
		
		String cordinatorAddr = rReplica.getFrom();
		String[] ipPortArray = cordinatorAddr.split(" ");
		Casandra.NodeMessage nodeMsg = null;
		if(val!=null) {
			//send read request to replica
			//Socket socket = new Socket(addr,port);
			
			//Compose return value
			StringBuilder sb = new StringBuilder();
			sb.append(InetAddress.getLocalHost().getHostAddress().toString()).append(" ").append(PORT);
			Casandra.ReturnVal retVal = Casandra.ReturnVal.newBuilder().
											setKey(key).setValue(val.getValue()).setTimestamp(val.getTimeStamp())
											.setFrom(sb.toString()).setLevel(rReplica.getLevel())
											.build();
			nodeMsg = Casandra.NodeMessage.newBuilder().setReturnVal(retVal).build();
			
			
		}
		else {
			Casandra.SystemException sysExp = Casandra.SystemException.newBuilder().setMessage("Key not found").build();
			nodeMsg = Casandra.NodeMessage.newBuilder().setSystemException(sysExp).build();
			
			
		}
		nodeMsg.writeDelimitedTo(client.getOutputStream());
		client.close();
		if(mode.equals("hh") && hintMap.keySet().size() > 0){
			new Thread(new HintedhandoffHandler(hintMap, cordinatorAddr.split(" ")[0], Integer.parseInt(cordinatorAddr.split(" ")[1]), PORT)).start();
		}
	}


	public int getPrimaryNodeIndex(int key) {
		int index = key%getAllNodeList().size();
		return index;
	}

	private void fetchValueFromAllReplica(Casandra.NodeMessage nodeMessage,
			Socket client)  {
		// TODO Auto-generated method stub
		Casandra.ReadValue clientReadMsg = nodeMessage.getReadValue();
		//String fromAddr = clientReadMsg.getFrom();
		int cLevel = clientReadMsg.getLevel();
		int index = getPrimaryNodeIndex(clientReadMsg.getKey());
		
		int cnt = 0;
		ArrayList<Casandra.NodeMessage> readMsgs = new ArrayList<>();
		Boolean flag = false;
		long lastTimestamp = 0;
		boolean startReadRepair = false;
		InetAddress currentIp = null;
		try {
			currentIp = InetAddress.getLocalHost();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			//e1.printStackTrace();
		}
		//String ipPort = allNodeList.get(index%allNodeList.size());
		for(int i = 0; i<4; i++) { //sending read requests to all four replicas
			
			try {
				String ipPort = allNodeList.get(index);
				String[] ipPortArray = ipPort.split(" ");
	
				InetAddress addr = InetAddress.getByName(ipPortArray[0]);
				int port = Integer.parseInt(ipPortArray[1]);
	
				
				
				if(!addr.getHostAddress().equals(currentIp.getHostAddress()) || port!=PORT) {
					// send read request to replica
					Socket socket = new Socket(addr, port);
		
					// compose get request message
					StringBuilder sb = new StringBuilder();
					sb.append(InetAddress.getLocalHost().getHostAddress()).append(" ").append(PORT);
					Casandra.ReadFromReplica rReplica = Casandra.ReadFromReplica.newBuilder().
														setKey(clientReadMsg.getKey()).
														setFrom(sb.toString()).build();
					Casandra.NodeMessage nodeMsg = Casandra.NodeMessage.newBuilder().setReadFromReplica(rReplica).build();
					nodeMsg.writeDelimitedTo(socket.getOutputStream());
		
					//get the reply from a replica server. If exception, ignore and continue.
					Casandra.NodeMessage returnMsg = null;
					try {
						returnMsg = Casandra.NodeMessage.parseDelimitedFrom(socket.getInputStream());
						if(returnMsg!=null && returnMsg.hasReturnVal()) { //Read was successfull, add it to arrayList 
							if(lastTimestamp != 0 && returnMsg.getReturnVal().getTimestamp() != lastTimestamp) {
								startReadRepair = true;
							}else {
								lastTimestamp = returnMsg.getReturnVal().getTimestamp();
							}
								
							readMsgs.add(returnMsg);
							cnt++;
						}
					}
					catch(Exception e) {
						//timeout or some other exception
					}
					socket.close();
				}
				else {
					int key = clientReadMsg.getKey();
					ValueTuple val = getKeyValStore().get(key);
					Casandra.NodeMessage nodeM = null;
					if(val!=null) {
						Casandra.ReturnVal retVal = Casandra.ReturnVal.newBuilder().setKey(key).setValue(val.getValue()).setTimestamp(val.getTimeStamp()).build();
						nodeM = Casandra.NodeMessage.newBuilder().setReturnVal(retVal).build();
						if(lastTimestamp != 0 && nodeM.getReturnVal().getTimestamp() != lastTimestamp) {
							startReadRepair = true;
						}else {
							lastTimestamp = nodeM.getReturnVal().getTimestamp();
						}
						readMsgs.add(nodeM);
						cnt++;
					}
					
					
					
				}
				
				if(cnt==cLevel) {   //quorum achieved send returnVal msg to client
					flag = true;
					sendLatestReadToClient(client,readMsgs);
				}
				index++;
				if(index==allNodeList.size()) {
					index = 0;
				}
			}
			catch(IOException ioe) {
				index++;
				if(index==allNodeList.size()) {
					index = 0;
				}
			}
		}
		if(!flag) {//send error msg to client
			try {
			Casandra.SystemException exMsg = Casandra.SystemException.newBuilder().setMessage("Consistency level was not achieved").build();
			Casandra.NodeMessage nodeMsg = Casandra.NodeMessage.newBuilder().setSystemException(exMsg).build();
			
			//write error msg to client output stream.
			nodeMsg.writeDelimitedTo(client.getOutputStream());
			client.close();
			}
			catch(IOException e) {
				
			}
			
		}if( (startReadRepair ||(cnt>0 && cnt< 4)) && mode.equals("rr")) {
			new Thread(new ReadRepairHandler(clientReadMsg.getKey())).start();
		}
	}
	
	public void writeValue(String ipPort,int key, String value, long timestamp, Socket client) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(InetAddress.getLocalHost().getHostAddress()+"_"+PORT+"_"+"WriteLog.txt", true));
		bw.write(timestamp+" "+key+" "+value+"\n");
		bw.close();
		ValueTuple vt = new ValueTuple(value, timestamp);
		keyValStore.put(key, vt);
		System.out.println();
		if(client!=null){
			Casandra.AckWrite.Builder ackWrite = Casandra.AckWrite.newBuilder();
			ackWrite.setFrom(InetAddress.getLocalHost().getHostAddress() + " "+ PORT);
			Casandra.NodeMessage  nodeMsg = Casandra.NodeMessage.newBuilder().setAckWrite(ackWrite).build();
			
			nodeMsg.writeDelimitedTo(client.getOutputStream());
			client.close();
			if(mode.equals("hh") && hintMap.keySet().size() > 0){
				new Thread(new HintedhandoffHandler(hintMap, ipPort.split(" ")[0], Integer.parseInt(ipPort.split(" ")[1]), PORT)).start();
			}
		}
	}
	
	public void sendValueToAllReplica(Casandra.NodeMessage nm, Socket client) throws UnknownHostException{

		Casandra.Put putMsg = nm.getPut();
		Casandra.PutReplicaServer.Builder putReplicaMsg = Casandra.PutReplicaServer
				.newBuilder();
		putReplicaMsg.setKey(putMsg.getKey());
		putReplicaMsg.setValue(putMsg.getValue());
		putReplicaMsg.setTimestamp(new Date().getTime());
		putReplicaMsg.setFrom(InetAddress.getLocalHost().getHostAddress() + " "
				+ PORT);
		int index = getPrimaryNodeIndex(putMsg.getKey());
		int count=0;
		for (int i = 0; i < 4; i++) {
			String ipPort = allNodeList.get(index);
			try {
				String[] ipPortArray = ipPort.split(" ");
				InetAddress addr = InetAddress.getByName(ipPortArray[0]);
				int port = Integer.parseInt(ipPortArray[1]);
				String ip = InetAddress.getLocalHost().getHostAddress();
				if(ip.equals(ipPortArray[0]) && port == PORT){
					writeValue(ipPort, putMsg.getKey(), putMsg.getValue(), putReplicaMsg.getTimestamp(), null);
					count++;
					if(count==putMsg.getLevel()){
						Casandra.AckWrite.Builder ack = Casandra.AckWrite.newBuilder();
						ack.setFrom(InetAddress.getLocalHost().getHostAddress() + " "+ PORT);
						Casandra.NodeMessage.Builder returnMsg = Casandra.NodeMessage.newBuilder();
						returnMsg.setAckWrite(ack.build());
						returnMsg.build().writeDelimitedTo(client.getOutputStream());
					}
					index++;
					if(index==allNodeList.size()) {
						index = 0;
					}
					continue;
				}
				// send read request to replica
				Socket socket = new Socket(addr, port);
				OutputStream os = socket.getOutputStream();
				Casandra.NodeMessage.Builder nodeMessage = Casandra.NodeMessage
						.newBuilder();
				nodeMessage.setPutReplicaServer(putReplicaMsg.build());
				nodeMessage.build().writeDelimitedTo(socket.getOutputStream());
				//socket.shutdownOutput();
				Casandra.NodeMessage ackMessage = Casandra.NodeMessage.parseDelimitedFrom(socket.getInputStream());
				socket.close();
				if(ackMessage.hasAckWrite()){
					count++;
					if(count==putMsg.getLevel()){
						Casandra.AckWrite.Builder ack = Casandra.AckWrite.newBuilder();
						ack.setFrom(InetAddress.getLocalHost().getHostAddress() + " "+ PORT);
						Casandra.NodeMessage.Builder returnMsg = Casandra.NodeMessage.newBuilder();
						returnMsg.setAckWrite(ack.build());
						returnMsg.build().writeDelimitedTo(client.getOutputStream());
					}
				}else if(mode.equals("hh")){
					writeHintedHandoff(ipPort,putMsg.getKey(),putMsg.getValue(),putReplicaMsg.getTimestamp());
				}
			} catch (IOException e) {
				if(mode.equals("hh")){
					writeHintedHandoff(ipPort,putMsg.getKey(),putMsg.getValue(),putReplicaMsg.getTimestamp());
				}
			}
			index++;
			if(index==allNodeList.size()) {
				index = 0;
			}
		}
		if(count<putMsg.getLevel()){
			// send error message to client.
			try{
				Casandra.NodeMessage.Builder error = Casandra.NodeMessage.newBuilder();
				Casandra.SystemException.Builder exception = Casandra.SystemException.newBuilder();
				exception.setMessage("Write consistency level not met");
				error.setSystemException(exception.build());
				error.build().writeDelimitedTo(client.getOutputStream());
			}catch(IOException e){
				
			}
		}

	}
	
	private void writeHintedHandoff(String ipPort,int key, String value, long timestamp){
		try{
			if(!hintMap.containsKey(ipPort)){
				hintMap.put(ipPort, new HashMap<Integer,ValueTuple>());
			}
			ValueTuple vt = new ValueTuple(value, timestamp);
			hintMap.get(ipPort).put(key, vt);
			
			writeHint(ipPort+" "+timestamp+" "+key+" "+value+"\n");
		}catch(IOException e){}
		
	}
	
	public synchronized void writeHint(String msg)throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(InetAddress.getLocalHost().getHostAddress()+"_"+PORT+"_"+"hints.txt",true));
		bw.write(msg);
		bw.close();
	} 

	private void sendLatestReadToClient(Socket clientSocket, ArrayList<Casandra.NodeMessage> readMsgs) throws IOException {
		
		
		Casandra.NodeMessage mostRecentRead = mostRecentRead(readMsgs);
		Casandra.ReturnVal retVal = mostRecentRead.getReturnVal();
		
		
		//compose returnVal reply  message
		StringBuilder sb = new StringBuilder();
		sb.append(InetAddress.getLocalHost().getHostAddress()).append(" ").append(PORT);
		Casandra.ReturnVal returnMsg = Casandra.ReturnVal.newBuilder()
										.setFrom(sb.toString()).setKey(retVal.getKey()).setValue(retVal.getValue()).
										setTimestamp(retVal.getTimestamp())
										.setLevel(retVal.getLevel()).build();
		Casandra.NodeMessage returnNodeMsg = Casandra.NodeMessage.newBuilder().setReturnVal(returnMsg).build();
		
		
		// send most recent read to client
		returnNodeMsg.writeDelimitedTo(clientSocket.getOutputStream());
		clientSocket.close();
		
	}
	

	private Casandra.NodeMessage mostRecentRead(ArrayList<Casandra.NodeMessage> readMsgs) {
		// TODO Auto-generated method stub
		long maxValue = -1;
	    int indexOfMaxValue = -1;
	    for(int i = 0; i < readMsgs.size(); i++){
	        if(readMsgs.get(i).getReturnVal().getTimestamp() > maxValue){
	            indexOfMaxValue = i;
	        }
	    }
	    return readMsgs.get(indexOfMaxValue);
	}


	private void initialize(String fileName) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader(new File(
				fileName)));
		this.allNodeList = new ArrayList<>();
		String line = br.readLine();
		while (line != null) {
			allNodeList.add(line);
			line = br.readLine();
		}
		br.close();
	}

	private static Server _instance;

	public static synchronized Server getInstance() throws IOException {
		if (_instance == null) {
			_instance = new Server();

		}
		return _instance;
	}

}

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.util.Scanner;

public class Client {

	
	private static String fileName="nodes";
	private static Socket socket;
	private static int PORT;
	private static List<String> allReplicaAddrs;
	private static String coordIp = null;
	private static int coordPort =0;
	
	public static void main(String[] args) throws UnknownHostException, IOException {
		
		allReplicaAddrs = new ArrayList<String>();
		getAllReplicaAddress(fileName);
		selectCordinator();
		System.out.println("Co-ordinator: "+coordIp+" : "+coordPort);
		sendReadWriteRequests();

	}
	
	/**
	 * Summary: Send read/write requests to co-ordinator
	 * Format of cmd: r <int_key> / w <int_key> <value> 
	 * @throws IOException
	 */
	private static void sendReadWriteRequests() throws IOException {
		
		Scanner input = new Scanner(System.in);
		String cmdLine = null;
		System.out.println("Read: R <consistency_level> <Key_int>  Write: W <consistency_level> <Key_int> <Value_String>");
		System.out.println("Type <exit> to exit");
		while(!(cmdLine = input.nextLine().trim().toLowerCase()).equals("exit")) {
			String[] args = cmdLine.split(" ");
			if(args.length ==3 || args.length>=4 ) {
				
				if(args[0].equals("r") || args[0].equals("w")) {
					
					try {
						try {
							socket = new Socket(coordIp,coordPort);
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							System.out.println("Coordinator not available");
							if(socket!=null) {
								socket.close();
							}
							continue;
						} catch (IOException e) {
							System.out.println("Coordinator not available");
							if(socket!=null) {
								socket.close();
							}
							continue;
						}
						if(args[0].equals("r") && args.length==3) { // READ BLOCK: READ CMD AND RECIEVE RETURNED VALUE HERE
							int cLevel = Integer.parseInt(args[1]);
							int key = Integer.parseInt(args[2]);
							if(key < 0) {
								System.out.println("Enter valid key between 0 to 255 (inclusive)");
								continue;
							}
							Casandra.ReadValue readMsg = Casandra.ReadValue.newBuilder().setKey(key).setLevel(cLevel).build();
							Casandra.NodeMessage nodeMsg = Casandra.NodeMessage.newBuilder().setReadValue(readMsg).build();
							nodeMsg.writeDelimitedTo(socket.getOutputStream());
							
							Casandra.NodeMessage nodeMsgReadValue = Casandra.NodeMessage.parseDelimitedFrom(socket.getInputStream());
							
							if(nodeMsgReadValue.hasReturnVal()) { // If valid response is returned
								Casandra.ReturnVal retMsg = nodeMsgReadValue.getReturnVal();
								System.out.println("Value for key "+key+ " is "+retMsg.getValue());
							}
							else if(nodeMsgReadValue.hasSystemException()){ // If exception has occured
								System.out.println("Exception occured: "+nodeMsgReadValue.getSystemException().getMessage());
							}
							
						}
						
						else if(args[0].equals("w") && args.length>=4) { // WRITE BLOCK: WRITE CMD AND RECIEVE ACK HERE
							int cLevel = Integer.parseInt(args[1]);
							int key = Integer.parseInt(args[2]);
							if(key < 0) {
								System.out.println("Enter valid key between 0 to 255 (inclusive)");
								continue;
							}
							String value = args[3];
							for(int i=4;i<args.length;i++) {
								value+=" "+args[i];
							}
							Casandra.Put writeMsg = Casandra.Put.newBuilder().setKey(key).setLevel(cLevel).setValue(value).build();
							Casandra.NodeMessage nodeMsg = Casandra.NodeMessage.newBuilder().setPut(writeMsg).build();
							nodeMsg.writeDelimitedTo(socket.getOutputStream());
							
							Casandra.NodeMessage nodeMsgReadValue = Casandra.NodeMessage.parseDelimitedFrom(socket.getInputStream());
							
							if(nodeMsgReadValue.hasAckWrite()) { // If valid response is returned
								Casandra.AckWrite ackWrite = nodeMsgReadValue.getAckWrite();
								System.out.println("Key value was successfully inserted: "+ackWrite.getFrom());
							}
							else if(nodeMsgReadValue.hasSystemException()){ //If exception has occured
								System.out.println("Exception occured: "+nodeMsgReadValue.getSystemException().getMessage());
							}
						}
						else {
							System.err.println("Invalid input format");
							//continue;
						}
						socket.close();
					}
					catch(NumberFormatException nfe) {
						System.err.println("Invalid input format");
						//continue;
					}
			
				}
				else {
					System.err.println("Invalid input format");
					//continue;
				}
			}
			
			else {
				System.err.println("Invalid input format");
				//continue;
			}
			if(socket!=null)
				socket.close();
			
		}
		
		input.close();
		if(socket!=null)
		socket.close();
		
		
		
		
	}

	

	/**
	 * Summary: Reads all server addresses and writes into allReplicaAddrs List
	 * @param fileName
	 * @throws IOException
	 */
	private static void getAllReplicaAddress(String fileName) throws IOException {

		System.out.println("Working Directory = " +
	              System.getProperty("user.dir"));
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		try {
			String line = br.readLine();
			
		    while(line!=null && !line.trim().equals("")) {
		    	allReplicaAddrs.add(line);
		    	line = br.readLine();
		    }
		    
		} finally {
		    br.close();
		}
	}
	
	
	/**
	 * Summary: Selects a coordinator for the client randomly
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * @throws NumberFormatException 
	 */
	private static void selectCordinator() throws NumberFormatException, UnknownHostException, IOException {
		// TODO Auto-generated method stub
		Random randomGen = new Random();
		System.out.println("Size: "+allReplicaAddrs.size());
		int index = randomGen.nextInt(allReplicaAddrs.size());
		String arrayItem = allReplicaAddrs.get(index);
		String[] ipPort = arrayItem.split(" ");
		coordIp = ipPort[0];
		coordPort = Integer.parseInt(ipPort[1]);
		//socket = new Socket(coordIp,coordPort);
	}
	
}

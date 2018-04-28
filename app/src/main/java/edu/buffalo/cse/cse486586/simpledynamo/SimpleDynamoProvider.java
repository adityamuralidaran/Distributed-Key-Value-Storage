package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Credentials;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Node;

public class SimpleDynamoProvider extends ContentProvider {

	//Code Source: simpleDHT Project 3
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String KEY = "key";
	static final String VALUE = "value";
    static final String OWNER = "owner";
    static final String MSG_TYPE = "type";
    static final String MSG_FROM = "from";


    // Types of message
    static final String TYPE_WRITE = "write";
    static final String TYPE_WRITE_REPLICA = "wreplica";
    static final String TYPE_WRITE_SUCCESS = "writesuccess";
    static final String TYPE_REPLICA_SUCCESS = "wreplicasuccess";
    static final String TYPE_READ_KEY = "readkey";
    static final String TYPE_READ_KEY_RESPONSE = "readkeyres";
    static final String TYPE_READ_ALL = "readall";
    static final String TYPE_READ_ALL_RESPONSE = "readallres";

    static final String  onSuccess = "success";
    static final String onFail = "failed";


	// hard coded ring structure
    static final String[] NODE_RING = new String[]{"5562","5556","5554","5558","5560"};



	//Code Source simpleDHT Project 3
	public static String DB_NAME = "GroupMessenger.db";
	public static String TABLE_NAME = "MessageHistory";
	public static String Create_Query = "CREATE TABLE " + TABLE_NAME +
			"(key TEXT PRIMARY KEY, value TEXT, owner TEXT);";
	public static String[] PROJECTIONS = new String[]{KEY,VALUE};

    // port number of successor AVD that contains the replicas
    public static HashMap<String, List<String>> SUCCESSOR = new HashMap<String, List<String>>(){{
        put("5562", Arrays.asList("5556","5554"));
        put("5556", Arrays.asList("5554","5558"));
        put("5554", Arrays.asList("5558","5560"));
        put("5558", Arrays.asList("5560","5562"));
        put("5560", Arrays.asList("5562","5556"));
    }};

    // Predecessor AVDs of which the current AVD store the replica.
    public static HashMap<String, List<String>> PREDECESSOR = new HashMap<String, List<String>>(){{
        put("5562", Arrays.asList("5560","5558"));
        put("5556", Arrays.asList("5562","5560"));
        put("5554", Arrays.asList("5556","5562"));
        put("5558", Arrays.asList("5554","5556"));
        put("5560", Arrays.asList("5558","5554"));
    }};

    // My port number
    public static String MYPORT = new String();

    public SQLiteDatabase db;

    // Lock to Read and Write with fairness
    // Source: https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/locks/ReentrantReadWriteLock.html
    public final static ReentrantReadWriteLock RW_LOCK = new ReentrantReadWriteLock(true);
    //public final static ReentrantLock WRITE_LOCK = new ReentrantLock();
    //public final static ReentrantLock READ_LOCK = new ReentrantLock();

    // To store result of query("*")
    public static MatrixCursor queryAllCursor;
    // To store result of query(key)
    public static MatrixCursor queryKeyCursor;

	// dbHelper class. Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteOpenHelper.html
	public static class dbHelper extends SQLiteOpenHelper {
		dbHelper(Context context){
			super(context,DB_NAME,null,1);
		}

		@Override
		public void onCreate(SQLiteDatabase db){
			db.execSQL(Create_Query);
		}

		@Override
		public void onUpgrade(SQLiteDatabase db, int old_version, int new_version){
			db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
			onCreate(db);
		}
	}

	@Override
	public boolean onCreate() {
		try {
			// Code Source: simpleDHT project 3
			Log.v(TAG, "into on create");
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			MYPORT = String.valueOf((Integer.parseInt(portStr)));
			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			}
			catch (IOException e) {
				Log.e(TAG, "Can't create a ServerSocket");
				//return;

			}


		}
		catch (Exception e){
			Log.e(TAG, "OnCreate - Exception");

		}
		// Code Source: Project 2b
		dbHelper help = new dbHelper(getContext());
		db = help.getWritableDatabase();
		return (db != null);
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        insertHelper(values);
		return uri;
	}

    public void insertHelper(ContentValues values){
	    try{
            String key = values.get(KEY).toString();
            String value = values.get(VALUE).toString();
            String keyHash = this.genHash(key);
            String node = "";

            for(int i = 0; i < NODE_RING.length; i++) {
                String nodeHash = this.genHash(NODE_RING[i]);
                if(i == 0){
                    String prePortHash = this.genHash(NODE_RING[NODE_RING.length-1]);
                    if((keyHash.compareTo(nodeHash) <= 0) ||
                            (keyHash.compareTo(nodeHash) > 0) && keyHash.compareTo(prePortHash) > 0){
                        node = NODE_RING[i];
                    }
                    break;
                }
                else{
                    String prePortHash = this.genHash(NODE_RING[i-1]);
                    if((keyHash.compareTo(nodeHash) < 0 && keyHash.compareTo(prePortHash) > 0) ||
                            (keyHash.compareTo(nodeHash) == 0)){
                        node = NODE_RING[i];
                    }
                    break;
                }
            }

            if(node.equals(MYPORT)){
                values.put(OWNER, node);
                writeHelper(values, TYPE_WRITE);
            }
            else{
                String msg = (new JSONObject().put(MSG_TYPE, TYPE_WRITE)
                        .put(KEY, key)
                        .put(VALUE, value)
                        .put(OWNER , node)
                        .put(MSG_FROM,MYPORT)).toString();
                String response = sendMessage(msg,node);
                if(response.equals(onFail)){
                    String newNode = SUCCESSOR.get(node).get(0);
                    sendMessage(msg,newNode);
                }
            }
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Insert - NoSuchAlgorithmException");
        }
        catch (JSONException e){
            Log.e(TAG, "Insert - JSON Exception");
        }
    }

    public void writeHelper(ContentValues values, String type){
        try {
            //Write lock acquired
            //RW_LOCK.writeLock().lock();

            Cursor cursor = db.query(TABLE_NAME, null, "key = '" + values.get(KEY).toString() + "'", null, null, null, null);
            if (cursor.getCount() < 1) {
                RW_LOCK.writeLock().lock();
                db.insert(TABLE_NAME, null, values);
                RW_LOCK.writeLock().unlock();
            }
            else {
                RW_LOCK.writeLock().lock();
                db.update(TABLE_NAME, values, "key = '" + values.get(KEY).toString() + "'", null);
                RW_LOCK.writeLock().unlock();
            }
            cursor.close();
            // Handle update
            if (type.equals(TYPE_WRITE)) {
                // Sending message to store replicas
                // ...
                String replicaMessage = (new JSONObject().put(MSG_TYPE, TYPE_WRITE_REPLICA)
                        .put(KEY, values.get(KEY).toString())
                        .put(VALUE, values.get(VALUE).toString())
                        .put(OWNER, values.get(OWNER).toString())
                        .put(MSG_FROM, MYPORT)).toString();
                List<String> replicaList = SUCCESSOR.get(values.get(OWNER).toString());
                for(String node: replicaList){
                    if(!node.equals(MYPORT)){
                        sendMessage(replicaMessage,node);
                    }
                }

            }

            // write lock released
            //RW_LOCK.writeLock().unlock();
        }
        catch (JSONException e){
            Log.e(TAG, "Insert - JSON Exception");
            RW_LOCK.writeLock().unlock();
        }

    }


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        if (selection.equals("@")) {
            RW_LOCK.readLock().lock();
            Cursor cursor = db.query(TABLE_NAME, PROJECTIONS, null, selectionArgs, null, null, sortOrder);
            RW_LOCK.readLock().unlock();
            return cursor;
        }
        else if(selection.equals("*")){
            //queryAllCursor = new MatrixCursor(new String[]{KEY, VALUE});
            // to be removed
            return null;
        }
        else{
            //queryKeyCursor = new MatrixCursor(new String[]{KEY, VALUE});
            return queryKeyHelper(projection, selection, selectionArgs, sortOrder);
        }
		//return null;
	}

	public Cursor queryKeyHelper(String[] projection, String key, String[] selectionArgs,
                                 String sortOrder){
        try {
            //String key = selection;
            String keyHash = this.genHash(key);
            String node = "";

            for(int i = 0; i < NODE_RING.length; i++) {
                String nodeHash = this.genHash(NODE_RING[i]);
                if(i == 0){
                    String prePortHash = this.genHash(NODE_RING[NODE_RING.length-1]);
                    if((keyHash.compareTo(nodeHash) <= 0) ||
                            (keyHash.compareTo(nodeHash) > 0) && keyHash.compareTo(prePortHash) > 0){
                        node = NODE_RING[i];
                    }
                    break;
                }
                else{
                    String prePortHash = this.genHash(NODE_RING[i-1]);
                    if((keyHash.compareTo(nodeHash) < 0 && keyHash.compareTo(prePortHash) > 0) ||
                            (keyHash.compareTo(nodeHash) == 0)){
                        node = NODE_RING[i];
                    }
                    break;
                }
            }

            if(node.equals(MYPORT)){
                RW_LOCK.readLock().lock();
                Cursor cursor =  db.query(TABLE_NAME, PROJECTIONS, "key = '" + key + "'", selectionArgs,
                        null, null, sortOrder);
                RW_LOCK.readLock().unlock();
                return cursor;
            }
            else{
                String msg = (new JSONObject().put(MSG_TYPE, TYPE_READ_KEY)
                        .put(KEY, key)
                        .put(MSG_FROM,MYPORT)).toString();
                String response = sendMessage(msg,node);
                if(response.equals(onFail)){
                    String newNode = SUCCESSOR.get(node).get(0);
                    response = sendMessage(msg,newNode);
                }
                if(!response.equals(onFail)){

                }
            }

        }
        catch (JSONException e){
            Log.e(TAG, "Insert - JSON Exception");
            RW_LOCK.writeLock().unlock();
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Insert Helper - NoSuchAlgorithmException");
            return new MatrixCursor(new String[]{KEY, VALUE});
        }
        // to be removed
        return null;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    // Message sending class
    public String sendMessage (String message, String sendTo){
        try {
            String port = String.valueOf((Integer.parseInt(sendTo) * 2));
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            socket.setSoTimeout(1000);
            //String msgToSend = message;
            DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
            outputStream.writeUTF(message);
            outputStream.flush();
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());
            String strReceived = inputStream.readUTF().trim();
            inputStream.close();

            //socket.close();
            return strReceived;
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
            return onFail;
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
            return onFail;
        }
    }

    // Server Class
	// Code Source simpleDHT Project 3
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			try {
				ServerSocket serverSocket = sockets[0];

				while (true) {
					Socket newSocket = serverSocket.accept();
					DataInputStream inputStream = new DataInputStream(newSocket.getInputStream());
					String strReceived = inputStream.readUTF().trim();
					publishProgress(strReceived);

					inputStream.close();
					newSocket.close();
				}
				//serverSocket.close();
			}
			catch (IOException e) {
				Log.e(TAG, "Server Socket IOException");
				e.printStackTrace();
			}
            /*catch (JSONException e){
                Log.e(TAG, "Server Task JSON Exception");
            }*/

			return null;
		}

		protected void onProgressUpdate(DataOutputStream outputStream, String...strings) {
			try {
            /*
             * The following code displays what is received in doInBackground().
             */
                String strReceived = strings[0].trim();
                JSONObject obj = new JSONObject(strReceived);
                String msgType = (String) obj.get(MSG_TYPE);

                // Handling Write to a node
                if(msgType.equals(TYPE_WRITE)){
                    ContentValues cv = new ContentValues();
                    cv.put(KEY, (String)obj.get(KEY));
                    cv.put(VALUE, (String)obj.get(VALUE));
                    cv.put(OWNER, (String)obj.get(OWNER));
                    writeHelper(cv,TYPE_WRITE);
                    // Sending back write reply
                    String reply = (new JSONObject().put(MSG_TYPE, TYPE_WRITE_SUCCESS)
                            .put(MSG_FROM,MYPORT)).toString();
                    //DataOutputStream outputStream = new DataOutputStream(newSocket.getOutputStream());
                    outputStream.writeUTF(reply);
                    outputStream.flush();
                }

                // Handling writing of replicas
                if(msgType.equals(TYPE_WRITE_REPLICA)){
                    ContentValues cv = new ContentValues();
                    cv.put(KEY, (String)obj.get(KEY));
                    cv.put(VALUE, (String)obj.get(VALUE));
                    cv.put(OWNER, (String)obj.get(OWNER));
                    writeHelper(cv,TYPE_WRITE_REPLICA);

                    String reply = (new JSONObject().put(MSG_TYPE, TYPE_REPLICA_SUCCESS)
                            .put(MSG_FROM,MYPORT)).toString();
                    //DataOutputStream outputStream = new DataOutputStream(newSocket.getOutputStream());
                    outputStream.writeUTF(reply);
                    outputStream.flush();
                }

                // Handling reading of a key
                if(msgType.equals(TYPE_READ_KEY)){
                    String key = (String) obj.get(KEY);
                    RW_LOCK.readLock().lock();
                    Cursor cursor =  db.query(TABLE_NAME, PROJECTIONS, "key = '" + key + "'", null,
                            null, null, null);
                    RW_LOCK.readLock().unlock();
                    String keyRes = "";
                    String valueRes = "";
                    while (cursor.moveToNext()) {
                        keyRes = cursor.getString(cursor.getColumnIndex(KEY));
                        valueRes = cursor.getString(cursor.getColumnIndex(VALUE));
                    }
                    cursor.close();
                    String reply = (new JSONObject()
                            .put(MSG_TYPE,TYPE_READ_KEY_RESPONSE)
                            .put(MSG_FROM,MYPORT)
                            .put(KEY,keyRes)
                            .put(VALUE,valueRes)).toString();
                    outputStream.writeUTF(reply);
                    outputStream.flush();
                }

			}
            catch (JSONException e){
                Log.e(TAG, "failed in onProgressUpdate - JSON Exception");
            }
			catch(Exception e){
				Log.e(TAG, "failed in onProgressUpdate ");
				e.printStackTrace();
			}

			return;
		}
	}



	// Client Class
	// code source : simpleDHT project 3
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {
				String port = String.valueOf((Integer.parseInt(msgs[1]) * 2));
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(port));
				String msgToSend = msgs[0];
				DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
				outputStream.writeUTF(msgToSend);
				outputStream.flush();
				//socket.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}

			return null;
		}
	}
}

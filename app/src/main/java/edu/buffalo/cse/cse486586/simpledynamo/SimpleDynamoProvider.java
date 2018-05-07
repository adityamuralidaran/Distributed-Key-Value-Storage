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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
import android.renderscript.ScriptIntrinsicYuvToRGB;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Node;

public class SimpleDynamoProvider extends ContentProvider {

	//Code Source: simpleDHT Project 3
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final int TIMEOUT = 1000;
	static final String KEY = "key";
	static final String VALUE = "value";
    static final String OWNER = "owner";
    static final String MSG_TYPE = "type";
    static final String MSG_FROM = "from";
    static final String MSG_QUERY_RES_COUNT = "rescount";
    static final String MSG_QUERY_RES_KEY = "k";
    static final String MSG_QUERY_RES_VALUE = "v";


    // Types of message
    static final String TYPE_WRITE = "write";
    static final String TYPE_WRITE_REPLICA = "wreplica";
    static final String TYPE_WRITE_SUCCESS = "writesuccess";
    static final String TYPE_REPLICA_SUCCESS = "wreplicasuccess";
    static final String TYPE_READ_KEY = "readkey";
    static final String TYPE_READ_KEY_RESPONSE = "readkeyres";
    static final String TYPE_READ_ALL = "readall";
    static final String TYPE_READ_ALL_RESPONSE = "readallres";
    static final String TYPE_DELETE_KEY = "deletekey";
    static final String TYPE_DELETE_KEY_RESPONSE = "deletekeyres";
    static final String TYPE_RECOVERY = "recovery";
    static final String TYPE_RECOVERY_RESPONSE = "recoveryres";

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

    // Holdback queue for all the write request during failure recovery.
    public static Queue<JSONObject> writeBuffer = new LinkedList<JSONObject>();

    // My port number
    public static String MYPORT = new String();

    public SQLiteDatabase db;

    // Lock to Read and Write with fairness
    // Source: https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/locks/ReentrantReadWriteLock.html
    public final static ReentrantReadWriteLock RW_LOCK = new ReentrantReadWriteLock(true);
    public final static ReentrantLock WRITE_LOCK = new ReentrantLock();
    public final static ReentrantLock WRITE_REPLICA_LOCK = new ReentrantLock();
    public final static ReentrantLock READ_LOCK = new ReentrantLock();
    public final static ReentrantLock DELETE_LOCK = new ReentrantLock();
    public final static ReentrantLock RECOVERY_LOCK = new ReentrantLock();

    public static boolean isRecoveryActive = false;
    public static int recoveryReply = 0;

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
			//onCreate(db);
            db.execSQL(Create_Query);
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
            // Code Source: Project 2b
            dbHelper help = new dbHelper(getContext());
            db = help.getWritableDatabase();

            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            }
            catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
                //return;
                e.printStackTrace();

            }

		}
		catch (Exception e){
			Log.e(TAG, "OnCreate - Exception");
			e.printStackTrace();
		}
		return (db != null);
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
	    try {
            String keyHash = this.genHash(selection);
            String node = "";

            for (int i = 0; i < NODE_RING.length; i++) {
                String nodeHash = this.genHash(NODE_RING[i]);
                if (i == 0) {
                    String prePortHash = this.genHash(NODE_RING[NODE_RING.length - 1]);
                    if ((keyHash.compareTo(nodeHash) <= 0) ||
                            (keyHash.compareTo(nodeHash) > 0) && keyHash.compareTo(prePortHash) > 0) {
                        node = NODE_RING[i];
                        break;
                    }
                } else {
                    String prePortHash = this.genHash(NODE_RING[i - 1]);
                    if ((keyHash.compareTo(nodeHash) < 0 && keyHash.compareTo(prePortHash) > 0) ||
                            (keyHash.compareTo(nodeHash) == 0)) {
                        node = NODE_RING[i];
                        break;
                    }
                }
            }

            String msg = (new JSONObject().put(MSG_TYPE, TYPE_DELETE_KEY)
                    .put(KEY, selection)
                    .put(MSG_FROM,MYPORT)).toString();

            synchronized (DELETE_LOCK){
                //sendMessage(msg,node);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node);
                DELETE_LOCK.wait(TIMEOUT);
            }
            List<String> replicaPorts = new ArrayList<String>(SUCCESSOR.get(node));
            for(String replica:replicaPorts ){
                synchronized (DELETE_LOCK){
                    //sendMessage(msg,replica);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, replica);
                    DELETE_LOCK.wait(TIMEOUT);
                }
            }
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Delete - NoSuchAlgorithmException");
        }
        catch (JSONException e){
            Log.e(TAG, "Insert - JSON Exception");
        }
        catch (InterruptedException e){
            Log.e(TAG, "Insert Helper - Interrupted Exception");
        }
		return 0;
	}

	public void deleteHelper(String key){
        //RW_LOCK.writeLock().lock();
        db.delete(TABLE_NAME,"key = '" +key + "'",null);
        //RW_LOCK.writeLock().unlock();
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
            //Log.v(TAG, "insertHelper: 1st element"+NODE_RING[0]);
            for(int i = 0; i < NODE_RING.length; i++) {
                String nodeHash = this.genHash(NODE_RING[i]);
                if(i == 0){
                    String prePortHash = this.genHash(NODE_RING[NODE_RING.length-1]);
                    if((keyHash.compareTo(nodeHash) <= 0) ||
                            (keyHash.compareTo(nodeHash) > 0 && keyHash.compareTo(prePortHash) > 0)){
                        node = NODE_RING[i];
                        break;
                    }
                }
                else{
                    String prePortHash = this.genHash(NODE_RING[i-1]);
                    if((keyHash.compareTo(nodeHash) < 0 && keyHash.compareTo(prePortHash) > 0) ||
                            (keyHash.compareTo(nodeHash) == 0)){
                        node = NODE_RING[i];
                        break;
                    }
                }
            }

            List<String> replicaPorts = new ArrayList<String>(SUCCESSOR.get(node));
            Collections.reverse(replicaPorts);
            for(String replica: replicaPorts){
                if(replica.equals(MYPORT) && !isRecoveryActive){
                    ContentValues cv = new ContentValues();
                    cv.put(KEY, key);
                    cv.put(VALUE, value);
                    cv.put(OWNER, node);
                    writeHelper(cv,TYPE_WRITE_REPLICA,MYPORT);
                }
                else {
                    synchronized (WRITE_REPLICA_LOCK) {
                        String msg = (new JSONObject().put(MSG_TYPE, TYPE_WRITE_REPLICA)
                                .put(KEY, key)
                                .put(VALUE, value)
                                .put(OWNER, node)
                                .put(MSG_FROM, MYPORT)).toString();
                        String res = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, replica)).get();
                        if (!res.equals(onFail))
                            WRITE_REPLICA_LOCK.wait(TIMEOUT);
                    }
                }
            }

            if(node.equals(MYPORT) && !isRecoveryActive){
                ContentValues cv = new ContentValues();
                cv.put(KEY, key);
                cv.put(VALUE, value);
                cv.put(OWNER, node);
                writeHelper(cv,TYPE_WRITE,MYPORT);
            }
            else {
                synchronized (WRITE_LOCK) {
                    String msg = (new JSONObject().put(MSG_TYPE, TYPE_WRITE)
                            .put(KEY, key)
                            .put(VALUE, value)
                            .put(OWNER, node)
                            .put(MSG_FROM, MYPORT)).toString();
                    String res = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node)).get();
                    if (!res.equals(onFail))
                        WRITE_LOCK.wait(TIMEOUT);
                }
            }
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "Insert - NoSuchAlgorithmException");
        }
        catch (JSONException e){
            Log.e(TAG, "Insert - JSON Exception");
        }
        catch (InterruptedException e){
            Log.e(TAG, "Insert Helper - Interrupted Exception");
        }
        catch (Exception e){
            Log.e(TAG, "Insert Helper - Exception");
        }
    }

    public void writeHelper(ContentValues values, String type, String from){
        try {

            Log.v(TAG, "WriteHelper Insert: key= "+values.get(KEY).toString()+ " value= "+values.get(VALUE).toString()+" owner: "+
                    values.get(OWNER).toString()+" from: " + from);
            db.insertWithOnConflict(TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
        }
        catch (Exception e) {
            Log.e(TAG, "Write helper - Exception");
            e.printStackTrace();
        }
    }


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        if (selection.equals("@")) {
            while(isRecoveryActive){}
            Cursor cursor = db.query(TABLE_NAME, PROJECTIONS, null, selectionArgs, null, null, sortOrder);
            return cursor;
        }
        else if(selection.equals("*")){

            return queryAllHelper();
        }
        else{
            return queryKeyHelper(selection, selectionArgs, sortOrder);
        }
	}

	public Cursor queryKeyHelper(String key, String[] selectionArgs, String sortOrder){
        synchronized (READ_LOCK) {
            try {
                String keyHash = this.genHash(key);
                String node = "";

                for(int i = 0; i < NODE_RING.length; i++) {
                    String nodeHash = this.genHash(NODE_RING[i]);
                    if(i == 0){
                        String prePortHash = this.genHash(NODE_RING[NODE_RING.length-1]);
                        if((keyHash.compareTo(nodeHash) <= 0) ||
                                (keyHash.compareTo(nodeHash) > 0) && keyHash.compareTo(prePortHash) > 0){
                            node = NODE_RING[i];
                            break;
                        }
                    }
                    else{
                        String prePortHash = this.genHash(NODE_RING[i-1]);
                        if((keyHash.compareTo(nodeHash) < 0 && keyHash.compareTo(prePortHash) > 0) ||
                                (keyHash.compareTo(nodeHash) == 0)){
                            node = NODE_RING[i];
                            break;
                        }
                    }
                }


                String msg = (new JSONObject().put(MSG_TYPE, TYPE_READ_KEY)
                        .put(KEY, key)
                        .put(MSG_FROM,MYPORT)).toString();
                List<String> nodes = new ArrayList<String>(Arrays.asList(SUCCESSOR.get(node).get(1),SUCCESSOR.get(node).get(0),node));
                for(String readNode:nodes) {
                    Log.v(TAG, "queryKeyHelper: key = " + key + " readFrom: " + readNode);
                    if (readNode.equals(MYPORT) && !isRecoveryActive) {
                        Cursor cursor = db.query(TABLE_NAME, PROJECTIONS, "key = '" + key + "'", null,
                                null, null, null);
                        return cursor;
                    }
                    else {

                            String response = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, readNode)).get();
                            Log.v(TAG, "waiting for key = " + key + " reading from: " + readNode);
                            Log.v(TAG, "Resuming after wait for key = " + key + " reading from: " + readNode);
                            Log.v(TAG, "queryKeyHelper: Response: "+ response);
                            if (!response.equals(onFail)) {
                                JSONObject obj = new JSONObject(response);
                                String msgType = (String) obj.get(MSG_TYPE);
                                String from = (String) obj.get(MSG_FROM);
                                //if(msgType.equals(TYPE_READ_KEY_RESPONSE)){
                                queryKeyCursor = new MatrixCursor(new String[]{KEY, VALUE});
                                String keyRes = (String) obj.get(KEY);
                                String valueRes = (String) obj.get(VALUE);
                                Log.v(TAG, "queryKey Response: key = "+keyRes+" Response From: "+ from);
                                queryKeyCursor.addRow(new Object[]{keyRes, valueRes});
                                //}
                                //READ_LOCK.wait(TIMEOUT);
                                return queryKeyCursor;
                            }
                            else {
                                continue;
                            }

                    }
                }
            }
            catch (JSONException e){
                Log.e(TAG, "query key helper - JSON Exception");
                return new MatrixCursor(new String[]{KEY, VALUE});
            }
            catch (NoSuchAlgorithmException e){
                Log.e(TAG, "query key helper - NoSuchAlgorithmException");
                return new MatrixCursor(new String[]{KEY, VALUE});
            }
            catch (InterruptedException e){
                Log.e(TAG, "query key helper - Interrupted Exception");
                return new MatrixCursor(new String[]{KEY, VALUE});
            }
            catch (Exception e){
                Log.e(TAG, "query key helper - Exception");
                e.printStackTrace();
                return new MatrixCursor(new String[]{KEY, VALUE});
            }
            // to be removed
            return new MatrixCursor(new String[]{KEY, VALUE});
        }
    }

    public Cursor queryAllHelper(){
        try {
            queryAllCursor = new MatrixCursor(new String[]{KEY, VALUE});
            Cursor cursor = db.query(TABLE_NAME, PROJECTIONS, null, null, null, null, null);

            while (cursor.moveToNext()) {
                String keyRes = cursor.getString(cursor.getColumnIndex(KEY));
                String valueRes = cursor.getString(cursor.getColumnIndex(VALUE));
                queryAllCursor.addRow(new Object[]{keyRes,valueRes});
            }

            String msg = (new JSONObject().put(MSG_TYPE, TYPE_READ_ALL)
                    .put(MSG_FROM,MYPORT)).toString();
            for(String node:NODE_RING) {
                if(!node.equals(MYPORT)) {
                    synchronized (READ_LOCK){
                        String response = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node)).get();
                        if(!response.equals(onFail)){
                            // Append keys to queryAllCursor.
                                READ_LOCK.wait(TIMEOUT);
                        }
                    }
                }
            }

            return queryAllCursor;
        }
        catch (JSONException e){
            Log.e(TAG, "query all - JSON Exception");
            return new MatrixCursor(new String[]{KEY, VALUE});
        }
        catch (InterruptedException e){
            Log.e(TAG, "query all - Interrupted Exception");
            return new MatrixCursor(new String[]{KEY, VALUE});
        }
        catch (Exception e){
            Log.e(TAG, "query all - Exception");
            return new MatrixCursor(new String[]{KEY, VALUE});
        }

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

    public void recoveryHelper(){
        try{
            ContentValues cv = new ContentValues();
            cv.put(KEY, "dummykey");
            cv.put(VALUE, "dummyval");
            cv.put(OWNER, MYPORT);
            db.insert(TABLE_NAME, null, cv);
            // Failure recovery.
            Cursor cursor = db.query(TABLE_NAME, PROJECTIONS, null, null, null, null, null);
            if (cursor.getCount() > 1) {
                isRecoveryActive = true;
                Log.v(TAG, "Starting Recover Keys");
                db.delete(TABLE_NAME, null, null);

                // Get my data
                List<String> successors = SUCCESSOR.get(MYPORT);
                for(String port: successors){
                    String msg = (new JSONObject().put(MSG_TYPE, TYPE_RECOVERY)
                            .put(OWNER, MYPORT)
                            .put(MSG_FROM,MYPORT)).toString();
                    //synchronized (RECOVERY_LOCK){
                        String res = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port)).get();
                        if(!res.equals(onFail)) {
                            //RECOVERY_LOCK.wait(TIMEOUT);
                            break;
                        }
                    //}
                }

                // Get the replicas to be stored in this node
                List<String> predecessor = PREDECESSOR.get(MYPORT);
                for(String port: predecessor){
                    String msg = (new JSONObject().put(MSG_TYPE, TYPE_RECOVERY)
                            .put(OWNER, port)
                            .put(MSG_FROM,MYPORT)).toString();
                    //synchronized (RECOVERY_LOCK){
                        String res = (new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port)).get();
                        if(!res.equals(onFail)) {
                            //RECOVERY_LOCK.wait(TIMEOUT);
                            continue;
                        }
                        else{
                            List<String> recoverySuccessor = SUCCESSOR.get(port);
                            for(String node: recoverySuccessor){
                                if(!node.equals(MYPORT)) {
                                    //synchronized (RECOVERY_LOCK) {
                                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, node);
                                        //sendMessage(msg,port);
                                        //RECOVERY_LOCK.wait(TIMEOUT);
                                        break;
                                    //}
                                }
                            }
                        }
                    //}
                }
                Log.v(TAG, "Ending Recover Keys");
            }
            else{
                db.delete(TABLE_NAME, null, null);
            }
        }
        catch (Exception e){
            Log.e(TAG, "recoveryHelper: Exception");
            e.printStackTrace();
        }
    }


    // Server Class
	// Code Source simpleDHT Project 3
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			try {
				ServerSocket serverSocket = sockets[0];

				// Recovery operation
				recoveryHelper();

				while (true) {
					Socket newSocket = serverSocket.accept();
					DataInputStream inputStream = new DataInputStream(newSocket.getInputStream());
					String strReceived = inputStream.readUTF().trim();

					JSONObject obj = new JSONObject(strReceived);
                    String msgType = (String) obj.get(MSG_TYPE);
                    String from = (String) obj.get(MSG_FROM);

                    if (!isRecoveryActive) {
                        DataOutputStream outputStream = new DataOutputStream(newSocket.getOutputStream());
                        if(msgType.equals(TYPE_READ_KEY)){
                            String key = (String) obj.get(KEY);
                            Cursor cursor =  db.query(TABLE_NAME, PROJECTIONS, "key = '" + key + "'", null,
                                    null, null, null);
                            String keyRes = "";
                            String valueRes = "";
                            while (cursor.moveToNext()) {
                                keyRes = cursor.getString(cursor.getColumnIndex(KEY));
                                valueRes = cursor.getString(cursor.getColumnIndex(VALUE));
                            }
                            cursor.close();
                            Log.v(TAG, "queryKey Response: key = "+keyRes+" Request From: "+from);
                            String reply = (new JSONObject()
                                    .put(MSG_TYPE,TYPE_READ_KEY_RESPONSE)
                                    .put(MSG_FROM,MYPORT)
                                    .put(KEY,keyRes)
                                    .put(VALUE,valueRes)).toString();

                            outputStream.writeUTF(reply);
                            outputStream.flush();
                        }
                        else {
                            outputStream.writeUTF(onSuccess);
                            outputStream.flush();
                        }
                        inputStream.close();
                        newSocket.close();

                        if(!msgType.equals(TYPE_READ_KEY))
                            publishProgress(strReceived);
                    }

                    else {
                        DataOutputStream outputStream = new DataOutputStream(newSocket.getOutputStream());
                        if(msgType.equals(TYPE_READ_KEY) || msgType.equals(TYPE_WRITE) || msgType.equals(TYPE_WRITE_REPLICA)) {
                            outputStream.writeUTF(onFail);
                            outputStream.flush();
                            inputStream.close();
                            newSocket.close();
                            if(!msgType.equals(TYPE_READ_KEY))
                                writeBuffer.add(obj);
                        }
                        else{
                            outputStream.writeUTF(onSuccess);
                            outputStream.flush();
                            inputStream.close();
                            newSocket.close();
                            publishProgress(strReceived);
                        }
                    }
				}
				//serverSocket.close();
			}
			catch (IOException e) {
				Log.e(TAG, "Server Socket IOException");
				e.printStackTrace();
			}
            catch (JSONException e){
                Log.e(TAG, "Server Task JSON Exception");
            }

			return null;
		}

		protected void onProgressUpdate(String...strings) {
			try {
            /*
             * The following code displays what is received in doInBackground().
             */
                String strReceived = strings[0].trim();
                JSONObject obj = new JSONObject(strReceived);
                String msgType = (String) obj.get(MSG_TYPE);
                String from = (String) obj.get(MSG_FROM);

                // Handling Write to a node
                if(msgType.equals(TYPE_WRITE)){
                    ContentValues cv = new ContentValues();
                    cv.put(KEY, (String)obj.get(KEY));
                    cv.put(VALUE, (String)obj.get(VALUE));
                    cv.put(OWNER, (String)obj.get(OWNER));
                    // message for Sending back write reply
                    String reply = (new JSONObject().put(MSG_TYPE, TYPE_WRITE_SUCCESS)
                            .put(MSG_FROM,MYPORT)).toString();
                    writeHelper(cv,TYPE_WRITE,from);


                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply, from);
                }

                // Handling Write response message
                else if(msgType.equals(TYPE_WRITE_SUCCESS)){
                    synchronized (WRITE_LOCK){
                        WRITE_LOCK.notify();
                    }
                }

                // Handling write replica
                else if(msgType.equals(TYPE_WRITE_REPLICA)){
                    ContentValues cv = new ContentValues();
                    cv.put(KEY, (String)obj.get(KEY));
                    cv.put(VALUE, (String)obj.get(VALUE));
                    cv.put(OWNER, (String)obj.get(OWNER));
                    // message for Sending back write reply
                    String reply = (new JSONObject().put(MSG_TYPE, TYPE_REPLICA_SUCCESS)
                            .put(MSG_FROM,MYPORT)).toString();
                    writeHelper(cv,TYPE_WRITE_REPLICA,from);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply, from);
                }

                // handling write replica rsponse
                else if(msgType.equals(TYPE_REPLICA_SUCCESS)){
                    synchronized (WRITE_REPLICA_LOCK){
                        WRITE_REPLICA_LOCK.notify();
                    }
                }

                // Handling reading of a key
                else if(msgType.equals(TYPE_READ_KEY)){
                    String key = (String) obj.get(KEY);
                    Cursor cursor =  db.query(TABLE_NAME, PROJECTIONS, "key = '" + key + "'", null,
                            null, null, null);
                    String keyRes = "";
                    String valueRes = "";
                    while (cursor.moveToNext()) {
                        keyRes = cursor.getString(cursor.getColumnIndex(KEY));
                        valueRes = cursor.getString(cursor.getColumnIndex(VALUE));
                    }
                    cursor.close();
                    Log.v(TAG, "queryKey Response: key = "+keyRes+" Request From: "+from);
                    String reply = (new JSONObject()
                            .put(MSG_TYPE,TYPE_READ_KEY_RESPONSE)
                            .put(MSG_FROM,MYPORT)
                            .put(KEY,keyRes)
                            .put(VALUE,valueRes)).toString();

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply, from);
                }

                // Handling read key response
                else if(msgType.equals(TYPE_READ_KEY_RESPONSE)){
                    synchronized (READ_LOCK) {
                        queryKeyCursor = new MatrixCursor(new String[]{KEY, VALUE});
                        String keyRes = (String) obj.get(KEY);
                        String valueRes = (String) obj.get(VALUE);
                        Log.v(TAG, "queryKey Response: key = "+keyRes+" Response From: "+ from);
                        queryKeyCursor.addRow(new Object[]{keyRes, valueRes});
                        READ_LOCK.notify();
                    }
                }

                // Handling reading of all the keys.
                else if(msgType.equals(TYPE_READ_ALL)){
                    Cursor cursor = db.query(TABLE_NAME, PROJECTIONS, null, null, null, null, null);
                    JSONObject obj1 = new JSONObject()
                            .put(MSG_TYPE,TYPE_READ_ALL_RESPONSE)
                            .put(MSG_FROM,MYPORT);
                    int i = 1;
                    // Code source project 3 simpleDHT
                    while (cursor.moveToNext()) {
                        String k = cursor.getString(cursor.getColumnIndex(KEY));
                        String v = cursor.getString(cursor.getColumnIndex(VALUE));
                        String keyName = MSG_QUERY_RES_KEY + Integer.toString(i);
                        String valueName = MSG_QUERY_RES_VALUE + Integer.toString(i);
                        obj1.put(keyName,k);
                        obj1.put(valueName,v);
                        i += 1;
                    }
                    obj1.put(MSG_QUERY_RES_COUNT,Integer.toString(i-1));
                    String reply = obj1.toString();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply, from);
                }

                // Handling read all response
                else if(msgType.equals(TYPE_READ_ALL_RESPONSE)){
                    synchronized (READ_LOCK) {
                        int resultCount = Integer.parseInt((String) obj.get(MSG_QUERY_RES_COUNT));
                        for (int i = 1; i <= resultCount; i++) {
                            String keyName = MSG_QUERY_RES_KEY + Integer.toString(i);
                            String valueName = MSG_QUERY_RES_VALUE + Integer.toString(i);
                            String ki = (String) obj.get(keyName);
                            String vi = (String) obj.get(valueName);
                            queryAllCursor.addRow(new Object[]{ki, vi});
                        }
                        READ_LOCK.notify();
                    }
                }

                // Handling Delete key message
                else if(msgType.equals(TYPE_DELETE_KEY)){
                    String key = (String) obj.get(KEY);
                    String reply = (new JSONObject().put(MSG_TYPE, TYPE_DELETE_KEY_RESPONSE)
                            .put(MSG_FROM,MYPORT)).toString();
                    deleteHelper(key);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply, from);
                }

                // Handling delete key response
                else if(msgType.equals(TYPE_DELETE_KEY_RESPONSE)){
                    synchronized (DELETE_LOCK){
                        DELETE_LOCK.notify();
                    }
                }

                // Handling failure recovery message
                else if(msgType.equals(TYPE_RECOVERY)){
                    Log.v(TAG, "onProgressUpdate: Recovery message recived from "+ from);
                    String owner = (String) obj.get(OWNER);
                    Cursor cursor =  db.query(TABLE_NAME, PROJECTIONS, "owner = '" + owner + "'", null,
                            null, null, null);
                    JSONObject obj1 = new JSONObject()
                            .put(MSG_TYPE,TYPE_RECOVERY_RESPONSE)
                            .put(MSG_FROM,MYPORT);
                    int i = 1;
                    // Code source project 3 simpleDHT
                    while (cursor.moveToNext()) {
                        String k = cursor.getString(cursor.getColumnIndex(KEY));
                        String v = cursor.getString(cursor.getColumnIndex(VALUE));
                        String keyName = MSG_QUERY_RES_KEY + Integer.toString(i);
                        String valueName = MSG_QUERY_RES_VALUE + Integer.toString(i);
                        obj1.put(keyName,k);
                        obj1.put(valueName,v);
                        i += 1;
                    }
                    obj1.put(OWNER,owner);
                    obj1.put(MSG_QUERY_RES_COUNT,Integer.toString(i-1));
                    String reply = obj1.toString();
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply, from);
                }

                // Handling failure recovery response
                if(msgType.equals(TYPE_RECOVERY_RESPONSE)){
                    String owner = (String) obj.get(OWNER);
                    //synchronized (RECOVERY_LOCK) {
                    int resultCount = Integer.parseInt((String) obj.get(MSG_QUERY_RES_COUNT));
                    for (int i = 1; i <= resultCount; i++) {
                        String keyName = MSG_QUERY_RES_KEY + Integer.toString(i);
                        String valueName = MSG_QUERY_RES_VALUE + Integer.toString(i);
                        ContentValues cv = new ContentValues();
                        cv.put(KEY, (String) obj.get(keyName));
                        cv.put(VALUE, (String) obj.get(valueName));
                        cv.put(OWNER, owner);
                        Log.v(TAG, "Recovered Key: key= "+(String) obj.get(keyName)+ " value= "+(String) obj.get(valueName)+" owner: "+
                                owner+" from: " + from);
                        writeHelper(cv,TYPE_RECOVERY_RESPONSE,from);
                    }
                    recoveryReply += 1;
                    if(recoveryReply == 3){
                        while(!writeBuffer.isEmpty()){
                            JSONObject obj1 = writeBuffer.poll();
                            ContentValues cv = new ContentValues();
                            cv.put(KEY, (String)obj1.get(KEY));
                            cv.put(VALUE, (String)obj1.get(VALUE));
                            cv.put(OWNER, (String)obj1.get(OWNER));
                            writeHelper(cv,TYPE_RECOVERY_RESPONSE,(String)obj1.get(MSG_FROM));
                        }
                        recoveryReply = 0;
                        isRecoveryActive = false;
                    }
                        //RECOVERY_LOCK.notify();
                    //}
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
	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			try {
				String port = String.valueOf((Integer.parseInt(msgs[1]) * 2));
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(port));
                socket.setSoTimeout(TIMEOUT);
				String msgToSend = msgs[0];

				DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
				outputStream.writeUTF(msgToSend);
				outputStream.flush();

                DataInputStream inputStream = new DataInputStream(socket.getInputStream());
                String strReceived = inputStream.readUTF().trim();
                inputStream.close();

                return strReceived;
				//socket.close();
			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
				return onFail;
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
				return onFail;
			}
			/*catch (JSONException e){
                Log.e(TAG, "Client task - JSON Exception");
                return onFail;
            }*/

			//return null;
		}
	}
}

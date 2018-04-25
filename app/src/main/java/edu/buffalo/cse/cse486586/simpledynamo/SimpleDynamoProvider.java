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
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDynamoProvider extends ContentProvider {

	//Code Source: Project 2b
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final String KEY = "key";
	static final String VALUE = "value";

	//Code Source Projecr 2b
	public static String DB_NAME = "GroupMessenger.db";
	public static String TABLE_NAME = "MessageHistory";
	public static String Create_Query = "CREATE TABLE " + TABLE_NAME +
			"(key TEXT PRIMARY KEY, value TEXT);";
	public SQLiteDatabase db;

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
			// Code Source: project 2b
			Log.v(TAG, "into on create");
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			final String myPort = String.valueOf((Integer.parseInt(portStr)));
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
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
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

	// Server Class
	// Code Source Project 2b
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			try {
				ServerSocket serverSocket = sockets[0];

				while (true) {
					Socket newSocket = serverSocket.accept();
					DataInputStream inputStream = new DataInputStream(newSocket.getInputStream());
					String strReceived = inputStream.readUTF().trim();
					//publishProgress(strReceived);
					inputStream.close();
					newSocket.close();
				}
				//serverSocket.close();
			}
			catch (IOException e) {
				Log.e(TAG, "Server Socket IOException");
				e.printStackTrace();
			}
			return null;
		}

		protected void onProgressUpdate(String...strings) {
			try {
            /*
             * The following code displays what is received in doInBackground().
             */
			}
			catch(Exception e){
				Log.e(TAG, "failed in onProgressUpdate ");
				e.printStackTrace();
			}

			return;
		}
	}

	// Client Class
	// code source : simpleDHT
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

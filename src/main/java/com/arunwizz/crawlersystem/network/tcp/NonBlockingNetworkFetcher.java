package com.arunwizz.crawlersystem.network.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arunwizz.crawlersystem.network.http.client.request.HTTPRequestWriter;
import com.arunwizz.crawlersystem.network.http.client.request.IRequestWriter;
import com.arunwizz.crawlersystem.network.http.client.response.ResponseHandler;
import com.arunwizz.crawlersystem.statistics.NetworkStatistics;
import com.arunwizz.crawlersystem.statistics.Statistician;

public class NonBlockingNetworkFetcher implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(NonBlockingNetworkFetcher.class);

	public static Charset charset = Charset.forName("UTF-8");
	public static CharsetEncoder encoder = charset.newEncoder();
	public static CharsetDecoder decoder = charset.newDecoder();

	private ResponseHandler responseHandler = null;
	private IRequestWriter requestWriter = null;

	// Create a selector and register two socket channels
	private Selector selector = null;

	public NonBlockingNetworkFetcher(ResponseHandler responseHandler)
			throws IOException {
		this.responseHandler = responseHandler;
		requestWriter = new HTTPRequestWriter();
		selector = Selector.open();
	}

	// Temporary storage for responses
	private Map<SelectionKey, StringBuilder> responseStorage = new HashMap<SelectionKey, StringBuilder>();
	private static final int BUFFER_SIZE = 1024;

	private LinkedBlockingQueue<URL> requestQueue = new LinkedBlockingQueue<URL>();

	synchronized public void get(URL url) {
		LOGGER.info("got request to download:" + url + " at " + new Date());
		LOGGER.debug("adding " + url + " into request queue");
		requestQueue.add(url);
		LOGGER.debug("waking up selector");
		selector.wakeup();

	}

	public void run() {

		LOGGER.info("thread running");
		while (true) {
			try {
				// Wait for an event
				LOGGER.debug("waiting for event on selector");
				int numOfKeysChanged = selector.select();
				LOGGER.info(numOfKeysChanged + " ready for processing");
				if (numOfKeysChanged == 0) {
					//no modified selection key, however some active selection key is still available
					Thread.sleep(1000);
//					continue;
				}
				// Get list of selection keys with pending events
				for (SelectionKey selectionKey : selector.selectedKeys()) {
					processSelectionKey(selectionKey);
				}
				// check if more request in queue
				checkForIncomingRequest();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				continue;
			} catch (InterruptedException e) {
				LOGGER.error(e.getMessage());
				continue;
			}
		}
	}

	private void checkForIncomingRequest() throws IOException {
		// check if any new registration
		URL url = null;
		LOGGER.info("checking if something in request queue");
		while ((url = requestQueue.poll()) != null) {
			LOGGER.info("found request for url: " + url);
			SocketChannel socketChannel = createSocketChannel(url.getHost(),
					url.getPort()==-1?80:url.getPort());
			LOGGER.debug("socket channel created for url");
			SelectionKey selectionKey = socketChannel.register(selector,
					socketChannel.validOps());
			LOGGER.debug("socket channel registered with selector");
			selectionKey.attach(url);
		}

	}

	private void processSelectionKey(SelectionKey selectionKey)
			throws IOException {
		// Since the ready operations are cumulative,
		// need to check readiness for each operation
		if (selectionKey.isValid() && selectionKey.isConnectable()) {
			Date startTime = new Date();
			finishConnection(selectionKey);
			long totalConnTime = new Date().getTime() - startTime.getTime();
			Statistician.put(selectionKey.attachment().toString(),
					NetworkStatistics.Parameter.CONN, totalConnTime);
			LOGGER.debug("Time to Connect " + totalConnTime + "ms");
		}
		if (selectionKey.isValid() && selectionKey.isReadable()) {
			Date startTime = new Date();
			readChannel(selectionKey);
			long totalReadTime = new Date().getTime() - startTime.getTime();
			Statistician.put(selectionKey.attachment().toString(),
					NetworkStatistics.Parameter.RES, totalReadTime);
			LOGGER.debug("Time to Read " + totalReadTime + "ms");
		}
		if (selectionKey.isValid() && selectionKey.isWritable()) {
			if (writeCompleted.contains(selectionKey)) {
				LOGGER.trace("writing already done");
				return;
			}
			Date startTime = new Date();
			writeChannel(selectionKey);
			long totalWriteTime = new Date().getTime() - startTime.getTime();
			Statistician.put(selectionKey.attachment().toString(),
					NetworkStatistics.Parameter.REQ, totalWriteTime);
			LOGGER.debug("Time to Write " + totalWriteTime + "ms");
			// HACK: put the selection key into hashset to avoid re-sending same request
			writeCompleted.add(selectionKey);
			LOGGER.debug("Writing complete, waiting for response now");
		}
	}

	// Creates a non-blocking socket channel for the specified host name and
	// port.
	// connect() is called on the new channel before it is returned.
	private static SocketChannel createSocketChannel(String hostName, int port)
			throws IOException {
		// Create a non-blocking socket channel
		LOGGER.debug("creating socket channel for " + hostName + ", " + port);
		SocketChannel sChannel = SocketChannel.open();
		sChannel.configureBlocking(false);
		// Send a connection request to the server; this method is non-blocking
		sChannel.connect(new InetSocketAddress(hostName, port));
		LOGGER.debug("returning channel for " + hostName + ", " + port);
		return sChannel;
	}

	private void finishConnection(SelectionKey selectionKey) throws IOException {
		String url = ((URL) selectionKey.attachment()).toString();
		LOGGER.info("channel connectable for " + url);
		// Get channel with connection request
		SocketChannel sChannel = (SocketChannel) selectionKey.channel();

		boolean success = sChannel.finishConnect();
		if (success) {
			LOGGER.info("connection successful for "
					+ selectionKey.attachment());
			// connection established, create storage for this selection key
			LOGGER.info("creating storage for " + url);
			responseStorage.put(selectionKey, new StringBuilder());
		} else {
			LOGGER.error("error establishing connection for " + url);
			// An error occurred; handle it
			// Unregister the channel with this selector
			selectionKey.cancel();
		}
	}

	private HashSet<SelectionKey> writeCompleted = new HashSet<SelectionKey>();

	private void writeChannel(SelectionKey selectionKey) {
		LOGGER.debug("writeChannel called for " + selectionKey.toString());

		// Get channel that's ready for more bytes
		SocketChannel sChannel = (SocketChannel) selectionKey.channel();

		// Create a direct buffer to get bytes from socket.
		// Direct buffers should be long-lived and be reused as much as
		// possible.
		ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);

	
		try {
			String getRequest = requestWriter.get(((URL)selectionKey.attachment()).toString());


			buf.put(getRequest.getBytes());

			// Prepare the buffer for reading by the socket
			buf.flip();

			// Write bytes
			int numBytesWritten = sChannel.write(buf);
			LOGGER.info("Request sent (" + numBytesWritten + ") bytes");
			LOGGER.debug(getRequest);

			// FIXME: this seems not working, selector is still returning this
			// channel for
			// writing

			selectionKey.interestOps(SelectionKey.OP_READ);
		} catch (IOException e) {
			LOGGER.error(e.getMessage());
		}
	}

	private ByteBuffer buf = ByteBuffer.allocateDirect(BUFFER_SIZE);

	int counter = 0;
	public void readChannel(SelectionKey selectionKey) {

		counter++;
		SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
		try {
			// Clear the buffer and read bytes from socket
			buf.clear();
			while (socketChannel.read(buf) > 0) {
				// To read the bytes, flip the buffer
				buf.flip();
				responseStorage.get(selectionKey).append(decoder.decode(buf));
				buf.clear();
			}
			
			LOGGER.debug("Invoking response handler "
					+ ResponseHandler.class.getName()
					+ " to take handle response received");
			responseHandler.handle(
					selectionKey);
	
			
			LOGGER.debug("entering readChannel: " + counter);
			
			//See its non blocking mode, read will immediately return with what ever availble 
			//at socket buffer, so at HTTPResponse layer we need to wait till we have read bytes read as 
			//per Content-size or chunk with 0 found. so pass the selection key to Response handler.

			LOGGER.debug("Removing this response string from local storage");

		} catch (IOException e) {
			// Connection may have been closed
			LOGGER.error(e.getMessage());
			// TODO: log it s
		}

	}

}

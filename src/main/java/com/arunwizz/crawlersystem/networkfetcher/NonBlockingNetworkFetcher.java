package com.arunwizz.crawlersystem.networkfetcher;

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

import com.arunwizz.crawlersystem.networkfetcher.responseprocessor.IResponseHandler;
import com.arunwizz.crawlersystem.networkfetcher.responseprocessor.ResponseHandler;
import com.arunwizz.crawlersystem.statistics.NetworkStatistics;
import com.arunwizz.crawlersystem.statistics.Statistician;

public class NonBlockingNetworkFetcher implements Runnable {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(NonBlockingNetworkFetcher.class);

	public static Charset charset = Charset.forName("UTF-8");
	public static CharsetEncoder encoder = charset.newEncoder();
	public static CharsetDecoder decoder = charset.newDecoder();

	private ResponseHandler responseHandler = null;

	// Create a selector and register two socket channels
	private Selector selector = null;

	public NonBlockingNetworkFetcher(ResponseHandler responseHandler)
			throws IOException {
		this.responseHandler = responseHandler;
		selector = Selector.open();
	}

	// Temporary storage for responses
	private Map<SelectionKey, StringBuilder> responseStorage = new HashMap<SelectionKey, StringBuilder>();

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
				int numOfKeysReady = selector.select();
				LOGGER.info(numOfKeysReady + " ready for processing");
				// Get list of selection keys with pending events
				for (SelectionKey selectionKey : selector.selectedKeys()) {
					processSelectionKey(selectionKey);
				}
				// check if more request in queue
				checkForIncomingRequest();
			} catch (IOException e) {
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
					url.getPort());
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
			LOGGER.debug("Time to write " + totalConnTime + "ms");
		}
		if (selectionKey.isValid() && selectionKey.isReadable()) {
			Date startTime = new Date();
			readChannel(selectionKey);
			long totalReadTime = new Date().getTime() - startTime.getTime();
			Statistician.put(selectionKey.attachment().toString(),
					NetworkStatistics.Parameter.RES, totalReadTime);
			LOGGER.debug("Time to write " + totalReadTime + "ms");
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
			LOGGER.debug("Time to write " + totalWriteTime + "ms");
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
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);

		try {
			// Fill the buffer with the bytes to write;
			// see Putting Bytes into a ByteBuffer
			String getRequest = "GET "
					+ ((URL) selectionKey.attachment()).getPath()
					+ " HTTP/1.1\r\n" + "Host: "
					+ ((URL) selectionKey.attachment()).getHost() + "\r\n"
					+ "User-Agent: CanopusBot/0.1 (Ubuntu 11.10; Linux x86_64)"
					+ "\r\n" + "From: arunwizz@gmail.com" + "\r\n\r\n";

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

	private ByteBuffer buf = ByteBuffer.allocateDirect(128);

	public void readChannel(SelectionKey selectionKey) {

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
			// reading completed
			socketChannel.close();
			selectionKey.cancel();
			LOGGER.debug("Invoking response handler "
					+ IResponseHandler.class.getName()
					+ " to take handle response received");
			responseHandler.handle(selectionKey.attachment().toString(),
					responseStorage.get(selectionKey).toString());
			LOGGER.debug("Removing this response string from local storage");
			responseStorage.remove(selectionKey);

		} catch (IOException e) {
			// Connection may have been closed
			LOGGER.error(e.getMessage());
			// TODO: log it s
		}

	}

}

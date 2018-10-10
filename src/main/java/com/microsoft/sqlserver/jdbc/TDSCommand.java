package com.microsoft.sqlserver.jdbc;

import com.microsoft.sqlserver.jdbc.timeouts.TimeoutPoller;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TDSCommand encapsulates an interruptable TDS conversation.
 *
 * A conversation may consist of one or more TDS request and response messages.
 * A command may be interrupted at any point, from any thread, and for any
 * reason. Acknowledgement and handling of an interrupt is fully encapsulated by
 * this class.
 *
 * Commands may be created with an optional timeout (in seconds). Timeouts are
 * implemented as a form of interrupt, where the interrupt event occurs when the
 * timeout period expires. Currently, only the time to receive the response from
 * the channel counts against the timeout period.
 */
public abstract class TDSCommand {
	abstract boolean doExecute() throws SQLServerException;

	final static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Command");
	private final String logContext;

	final String getLogContext() {
		return logContext;
	}

	private String traceID;

	final public String toString() {
		if (traceID == null)
			traceID = "TDSCommand@" + Integer.toHexString(hashCode()) + " (" + logContext + ")";
		return traceID;
	}

	final void log(Level level, String message) {
		logger.log(level, toString() + ": " + message);
	}

	// TDS channel accessors
	// These are set/reset at command execution time.
	// Volatile ensures visibility to execution thread and interrupt thread
	private volatile TDSWriter tdsWriter;
	private volatile TDSReader tdsReader;

	protected TDSWriter getTDSWriter() {
		return tdsWriter;
	}

	// Lock to ensure atomicity when manipulating more than one of the following
	// shared interrupt state variables below.
	private final Object interruptLock = new Object();

	// Flag set when this command starts execution, indicating that it is
	// ready to respond to interrupts; and cleared when its last response packet is
	// received, indicating that it is no longer able to respond to interrupts.
	// If the command is interrupted after interrupts have been disabled, then the
	// interrupt is ignored.
	private volatile boolean interruptsEnabled = false;

	protected boolean getInterruptsEnabled() {
		return interruptsEnabled;
	}

	protected void setInterruptsEnabled(boolean interruptsEnabled) {
		synchronized (interruptLock) {
			this.interruptsEnabled = interruptsEnabled;
		}
	}

	// Flag set to indicate that an interrupt has happened.
	private volatile boolean wasInterrupted = false;

	private boolean wasInterrupted() {
		return wasInterrupted;
	}

	// The reason for the interrupt.
	private volatile String interruptReason = null;

	// Flag set when this command's request to the server is complete.
	// If a command is interrupted before its request is complete, it is the
	// executing
	// thread's responsibility to send the attention signal to the server if
	// necessary.
	// After the request is complete, the interrupting thread must send the
	// attention signal.
	private volatile boolean requestComplete;

	protected boolean getRequestComplete() {
		return requestComplete;
	}

	protected void setRequestComplete(boolean requestComplete) {
		synchronized (interruptLock) {
			this.requestComplete = requestComplete;
		}
	}

	// Flag set when an attention signal has been sent to the server, indicating
	// that a
	// TDS packet containing the attention ack message is to be expected in the
	// response.
	// This flag is cleared after the attention ack message has been received and
	// processed.
	private volatile boolean attentionPending = false;

	boolean attentionPending() {
		return attentionPending;
	}

	// Flag set when this command's response has been processed. Until this flag is
	// set,
	// there may be unprocessed information left in the response, such as
	// transaction
	// ENVCHANGE notifications.
	private volatile boolean processedResponse;

	protected boolean getProcessedResponse() {
		return processedResponse;
	}

	protected void setProcessedResponse(boolean processedResponse) {
		synchronized (interruptLock) {
			this.processedResponse = processedResponse;
		}
	}

	// Flag set when this command's response is ready to be read from the server and
	// cleared
	// after its response has been received, but not necessarily processed, up to
	// and including
	// any attention ack. The command's response is read either on demand as it is
	// processed,
	// or by detaching.
	private volatile boolean readingResponse;
	private int queryTimeoutSeconds;
	private int cancelQueryTimeoutSeconds;
	private TdsTimeoutCommand timeoutCommand;

	protected int getQueryTimeoutSeconds() {
		return this.queryTimeoutSeconds;
	}

	protected int getCancelQueryTimeoutSeconds() {
		return this.cancelQueryTimeoutSeconds;
	}

	final boolean readingResponse() {
		return readingResponse;
	}

	/**
	 * Creates this command with an optional timeout.
	 *
	 * @param logContext     the string describing the context for this command.
	 * @param timeoutSeconds (optional) the time before which the command must
	 *                       complete before it is interrupted. A value of 0 means
	 *                       no timeout.
	 */
	TDSCommand(String logContext, int queryTimeoutSeconds, int cancelQueryTimeoutSeconds) {
		this.logContext = logContext;
		this.queryTimeoutSeconds = queryTimeoutSeconds;
		this.cancelQueryTimeoutSeconds = cancelQueryTimeoutSeconds;
	}

	/**
	 * Executes this command.
	 *
	 * @param tdsWriter
	 * @param tdsReader
	 * @throws SQLServerException on any error executing the command, including
	 *                            cancel or timeout.
	 */

	boolean execute(TDSWriter tdsWriter, TDSReader tdsReader) throws SQLServerException {
		this.tdsWriter = tdsWriter;
		this.tdsReader = tdsReader;
		assert null != tdsReader;
		try {
			return doExecute(); // Derived classes implement the execution details
		} catch (SQLServerException e) {
			try {
				// If command execution threw an exception for any reason before the request
				// was complete then interrupt the command (it may already be interrupted)
				// and close it out to ensure that any response to the error/interrupt
				// is processed.
				// no point in trying to cancel on a closed connection.
				if (!requestComplete && !tdsReader.getConnection().isClosed()) {
					interrupt(e.getMessage());
					onRequestComplete();
					close();
				}
			} catch (SQLServerException interruptException) {
				if (logger.isLoggable(Level.FINE))
					logger.fine(this.toString() + ": Ignoring error in sending attention: "
							+ interruptException.getMessage());
			}
			// throw the original exception even if trying to interrupt fails even in the
			// case
			// of trying to send a cancel to the server.
			throw e;
		}
	}

	/**
	 * Provides sane default response handling.
	 *
	 * This default implementation just consumes everything in the response message.
	 */
	void processResponse(TDSReader tdsReader) throws SQLServerException {
		if (logger.isLoggable(Level.FINEST))
			logger.finest(this.toString() + ": Processing response");
		try {
			TDSParser.parse(tdsReader, getLogContext());
		} catch (SQLServerException e) {
			if (SQLServerException.DRIVER_ERROR_FROM_DATABASE != e.getDriverErrorCode())
				throw e;

			if (logger.isLoggable(Level.FINEST))
				logger.finest(this.toString() + ": Ignoring error from database: " + e.getMessage());
		}
	}

	/**
	 * Clears this command from the TDS channel so that another command can execute.
	 *
	 * This method does not process the response. It just buffers it in memory,
	 * including any attention ack that may be present.
	 */
	final void detach() throws SQLServerException {
		if (logger.isLoggable(Level.FINEST))
			logger.finest(this + ": detaching...");

		// Read any remaining response packets from the server.
		// This operation may be timed out or cancelled from another thread.
		while (tdsReader.readPacket())
			;

		// Postcondition: the entire response has been read
		assert !readingResponse;
	}

	final void close() {
		if (logger.isLoggable(Level.FINEST))
			logger.finest(this + ": closing...");

		if (logger.isLoggable(Level.FINEST))
			logger.finest(this + ": processing response...");

		while (!processedResponse) {
			try {
				processResponse(tdsReader);
			} catch (SQLServerException e) {
				if (logger.isLoggable(Level.FINEST))
					logger.finest(this + ": close ignoring error processing response: " + e.getMessage());

				if (tdsReader.getConnection().isSessionUnAvailable()) {
					processedResponse = true;
					attentionPending = false;
				}
			}
		}

		if (attentionPending) {
			if (logger.isLoggable(Level.FINEST))
				logger.finest(this + ": processing attention ack...");

			try {
				TDSParser.parse(tdsReader, "attention ack");
			} catch (SQLServerException e) {
				if (tdsReader.getConnection().isSessionUnAvailable()) {
					if (logger.isLoggable(Level.FINEST))
						logger.finest(this + ": giving up on attention ack after connection closed by exception: " + e);
					attentionPending = false;
				} else {
					if (logger.isLoggable(Level.FINEST))
						logger.finest(this + ": ignored exception: " + e);
				}
			}

			// If the parser returns to us without processing the expected attention ack,
			// then assume that no attention ack is forthcoming from the server and
			// terminate the connection to prevent any other command from executing.
			if (attentionPending) {
				if (logger.isLoggable(Level.SEVERE)) {
					logger.severe(this.toString()
							+ ": expected attn ack missing or not processed; terminating connection...");
				}

				try {
					tdsReader.throwInvalidTDS();
				} catch (SQLServerException e) {
					if (logger.isLoggable(Level.FINEST))
						logger.finest(this + ": ignored expected invalid TDS exception: " + e);

					assert tdsReader.getConnection().isSessionUnAvailable();
					attentionPending = false;
				}
			}
		}

		// Postcondition:
		// Response has been processed and there is no attention pending -- the command
		// is closed.
		// Of course the connection may be closed too, but the command is done
		// regardless...
		assert processedResponse && !attentionPending;
	}

	/**
	 * Interrupts execution of this command, typically from another thread.
	 *
	 * Only the first interrupt has any effect. Subsequent interrupts are ignored.
	 * Interrupts are also ignored until enabled. If interrupting the command
	 * requires an attention signal to be sent to the server, then this method sends
	 * that signal if the command's request is already complete.
	 *
	 * Signalling mechanism is "fire and forget". It is up to either the execution
	 * thread or, possibly, a detaching thread, to ensure that any pending attention
	 * ack later will be received and processed.
	 *
	 * @param reason the reason for the interrupt, typically cancel or timeout.
	 * @throws SQLServerException if interrupting fails for some reason. This call
	 *                            does not throw the reason for the interrupt.
	 */
	void interrupt(String reason) throws SQLServerException {
		// Multiple, possibly simultaneous, interrupts may occur.
		// Only the first one should be recognized and acted upon.
		synchronized (interruptLock) {
			if (interruptsEnabled && !wasInterrupted()) {
				if (logger.isLoggable(Level.FINEST))
					logger.finest(this + ": Raising interrupt for reason:" + reason);

				wasInterrupted = true;
				interruptReason = reason;
				if (requestComplete)
					attentionPending = tdsWriter.sendAttention();

			}
		}
	}

	private boolean interruptChecked = false;

	/**
	 * Checks once whether an interrupt has occurred, and, if it has, throws an
	 * exception indicating that fact.
	 *
	 * Any calls after the first to check for interrupts are no-ops. This method is
	 * called periodically from this command's execution thread to notify the app
	 * when an interrupt has happened.
	 *
	 * It should only be called from places where consistent behavior can be ensured
	 * after the exception is thrown. For example, it should not be called at
	 * arbitrary times while processing the response, as doing so could leave the
	 * response token stream in an inconsistent state. Currently, response
	 * processing only checks for interrupts after every result or OUT parameter.
	 *
	 * Request processing checks for interrupts before writing each packet.
	 *
	 * @throws SQLServerException if this command was interrupted, throws the reason
	 *                            for the interrupt.
	 */
	final void checkForInterrupt() throws SQLServerException {
		// Throw an exception with the interrupt reason if this command was interrupted.
		// Note that the interrupt reason may be null. Checking whether the
		// command was interrupted does not require the interrupt lock since only one
		// of the shared state variables is being manipulated; interruptChecked is not
		// shared with the interrupt thread.
		if (wasInterrupted() && !interruptChecked) {
			interruptChecked = true;

			if (logger.isLoggable(Level.FINEST))
				logger.finest(this + ": throwing interrupt exception, reason: " + interruptReason);

			throw new SQLServerException(interruptReason, SQLState.STATEMENT_CANCELED, DriverError.NOT_SET, null);
		}
	}

	/**
	 * Notifies this command when no more request packets are to be sent to the
	 * server.
	 *
	 * After the last packet has been sent, the only way to interrupt the request is
	 * to send an attention signal from the interrupt() method.
	 *
	 * Note that this method is called when the request completes normally (last
	 * packet sent with EOM bit) or when it completes after being interrupted (0 or
	 * more packets sent with no EOM bit).
	 */
	final void onRequestComplete() throws SQLServerException {
		synchronized (interruptLock) {
			assert !requestComplete;

			if (logger.isLoggable(Level.FINEST))
				logger.finest(this + ": request complete");

			requestComplete = true;

			// If this command was interrupted before its request was complete then
			// we need to send the attention signal if necessary. Note that if no
			// attention signal is sent (i.e. no packets were sent to the server before
			// the interrupt happened), then don't expect an attention ack or any
			// other response.
			if (!interruptsEnabled) {
				assert !attentionPending;
				assert !processedResponse;
				assert !readingResponse;
				processedResponse = true;
			} else if (wasInterrupted()) {

				if (tdsWriter.isEOMSent()) {
					attentionPending = tdsWriter.sendAttention();
					readingResponse = attentionPending;
				} else {
					assert !attentionPending;
					readingResponse = tdsWriter.ignoreMessage();
				}

				processedResponse = !readingResponse;
			} else {
				assert !attentionPending;
				assert !processedResponse;
				readingResponse = true;
			}
		}
	}

	/**
	 * Notifies this command when the last packet of the response has been read.
	 *
	 * When the last packet is read, interrupts are disabled. If an interrupt
	 * occurred prior to disabling that caused an attention signal to be sent to the
	 * server, then an extra packet containing the attention ack is read.
	 *
	 * This ensures that on return from this method, the TDS channel is clear of all
	 * response packets for this command.
	 *
	 * Note that this method is called for the attention ack message itself as well,
	 * so we need to be sure not to expect more than one attention ack...
	 */
	final void onResponseEOM() throws SQLServerException {
		boolean readAttentionAck = false;

		// Atomically disable interrupts and check for a previous interrupt requiring
		// an attention ack to be read.
		synchronized (interruptLock) {
			if (interruptsEnabled) {
				if (logger.isLoggable(Level.FINEST))
					logger.finest(this + ": disabling interrupts");

				// Determine whether we still need to read the attention ack packet.
				//
				// When a command is interrupted, Yukon (and later) always sends a response
				// containing at least a DONE(ERROR) token before it sends the attention ack,
				// even if the command's request was not complete.
				readAttentionAck = attentionPending;

				interruptsEnabled = false;
			}
		}

		// If an attention packet needs to be read then read it. This should
		// be done outside of the interrupt lock to avoid unnecessarily blocking
		// interrupting threads. Note that it is remotely possible that the call
		// to readPacket won't actually read anything if the attention ack was
		// already read by TDSCommand.detach(), in which case this method could
		// be called from multiple threads, leading to a benign followup process
		// to clear the readingResponse flag.
		if (readAttentionAck)
			tdsReader.readPacket();

		readingResponse = false;
	}

	/**
	 * Notifies this command when the end of its response token stream has been
	 * reached.
	 *
	 * After this call, we are guaranteed that tokens in the response have been
	 * processed.
	 */
	final void onTokenEOF() {
		processedResponse = true;
	}

	/**
	 * Notifies this command when the attention ack (a DONE token with a special
	 * flag) has been processed.
	 *
	 * After this call, the attention ack should no longer be expected.
	 */
	final void onAttentionAck() {
		assert attentionPending;
		attentionPending = false;
	}

	/**
	 * Starts sending this command's TDS request to the server.
	 *
	 * @param tdsMessageType the type of the TDS message (RPC, QUERY, etc.)
	 * @return the TDS writer used to write the request.
	 * @throws SQLServerException on any error, including acknowledgement of an
	 *                            interrupt.
	 */
	final TDSWriter startRequest(byte tdsMessageType) throws SQLServerException {
		if (logger.isLoggable(Level.FINEST))
			logger.finest(this + ": starting request...");

		// Start this command's request message
		try {
			tdsWriter.startMessage(this, tdsMessageType);
		} catch (SQLServerException e) {
			if (logger.isLoggable(Level.FINEST))
				logger.finest(this + ": starting request: exception: " + e.getMessage());

			throw e;
		}

		// (Re)initialize this command's interrupt state for its current execution.
		// To ensure atomically consistent behavior, do not leave the interrupt lock
		// until interrupts have been (re)enabled.
		synchronized (interruptLock) {
			requestComplete = false;
			readingResponse = false;
			processedResponse = false;
			attentionPending = false;
			wasInterrupted = false;
			interruptReason = null;
			interruptsEnabled = true;
		}

		return tdsWriter;
	}

	/**
	 * Finishes the TDS request and then starts reading the TDS response from the
	 * server.
	 *
	 * @return the TDS reader used to read the response.
	 * @throws SQLServerException if there is any kind of error.
	 */
	final TDSReader startResponse() throws SQLServerException {
		return startResponse(false);
	}

	final TDSReader startResponse(boolean isAdaptive) throws SQLServerException {
		// Finish sending the request message. If this command was interrupted
		// at any point before endMessage() returns, then endMessage() throws an
		// exception with the reason for the interrupt. Request interrupts
		// are disabled by the time endMessage() returns.
		if (logger.isLoggable(Level.FINEST))
			logger.finest(this + ": finishing request");

		try {
			tdsWriter.endMessage();
		} catch (SQLServerException e) {
			if (logger.isLoggable(Level.FINEST))
				logger.finest(this + ": finishing request: endMessage threw exception: " + e.getMessage());

			throw e;
		}

		// If command execution is subject to timeout then start timing until
		// the server returns the first response packet.
		if (queryTimeoutSeconds > 0) {
			this.timeoutCommand = new TdsTimeoutCommand(queryTimeoutSeconds, this, null);
			TimeoutPoller.getTimeoutPoller().addTimeoutCommand(this.timeoutCommand);
		}

		if (logger.isLoggable(Level.FINEST))
			logger.finest(this.toString() + ": Reading response...");

		try {
			// Wait for the server to execute the request and read the first packet
			// (responseBuffering=adaptive) or all packets (responseBuffering=full)
			// of the response.
			if (isAdaptive) {
				tdsReader.readPacket();
			} else {
				while (tdsReader.readPacket())
					;
			}
		} catch (SQLServerException e) {
			if (logger.isLoggable(Level.FINEST))
				logger.finest(this.toString() + ": Exception reading response: " + e.getMessage());

			throw e;
		} finally {
			// If command execution was subject to timeout then stop timing as soon
			// as the server returns the first response packet or errors out.
			if (this.timeoutCommand != null) {
				TimeoutPoller.getTimeoutPoller().remove(this.timeoutCommand);
			}
		}

		return tdsReader;
	}
}
/**
 * 
 */
package src.proj.zoie.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.impl.indexing.DefaultIndexReaderDecorator;
import proj.zoie.impl.indexing.FileDataProvider;
import proj.zoie.impl.indexing.FileIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieSystem;
import src.proj.zoie.client.ZoiePerfClient;

/**
 * @author nnarkhed
 *
 */
public class MonitoredZoieSystem {

	private static final Logger log = Logger.getLogger(MonitoredZoieSystem.class);
	// input throttle numbers for modifying indexing load for performance numbers
	private String _ipPerfFile;
	// output file for tracking the indexing speed for performance numbers
	private String _opPerfFile;
	
	// batch size
	private int _batchSize;
	
	// batch delay
	private long _batchDelay;
	
	// directory containing indexing
	private File _idxDir;
	
	// directory containing data to be indexed
	private File _idxInput;
	
	// zoie system
	private ZoieSystem<IndexReader, File> _zoie;
	
	// data provider for indexing
	private FileDataProvider _provider;
	
	// thread that logs performance numbers for zoie indexing
	private PerfThread _thread;
	
	// variable to track if query performance should be changed
	private boolean _changeLoad;

	// list of throttle parameters to vary indexing load on zoie. Following two parameters unused if _changeLoad = false
	protected Stack<Long> _throttleParams = new Stack<Long>();
	
	// current throttle parameter value
	private long _throttle;

	// time period after which throttle parameter should be changed
	private static final long _throttleChangeDuration = 60000; // milliseconds
	
	// variable to decide if indexing performance should be tracked or no
	private boolean _trackIndexingPerf;

	public MonitoredZoieSystem(int batchSize, long batchDelay, File idxInput, File idxDir, boolean changeLoad, boolean trackPerf) {
		_idxInput = idxInput;
		_idxDir = idxDir;
		_batchSize = batchSize;
		_batchDelay = batchDelay;
		_changeLoad = changeLoad;
		_trackIndexingPerf = trackPerf;
	}
	
	/**
	 * @param ipPerfFile the file containing throttling parameters to vary indexing load
	 */
	public void setIndexingIpPerfFile(String ipPerfFile) {
		_ipPerfFile = ipPerfFile;
	}

	/**
	 * @param opPerfFile the file containing indexing performance numbers 
	 */
	public void setIndexingOpPerfFile(String opPerfFile) {
		_opPerfFile = opPerfFile;
	}

	public void loadIndexingThrottle(String ipFile) {
		BufferedReader br = null;
		try {
			br= new BufferedReader(new FileReader(ipFile));
		} catch (FileNotFoundException e) {
			log.error("Could not load queries file " + ipFile + "\n" + e.getMessage());
		}
		String line = null;
		try {
			while(((line = br.readLine()) != null) && !line.equals("")) {
				_throttleParams.push(new Long(Long.valueOf(line)));
			}
		} catch (IOException e) {
			log.error("Error reading wait timings from file " + ipFile + "\n " + e.getMessage());
		}
		System.out.println("Loaded " + _throttleParams.size() + " throttle timings from " + ipFile);
	}
	
	/**
	 * 
	 * @return The ZoieSystem object for the current performance configuration
	 */
	public void createZoie() {
		_zoie = new ZoieSystem<IndexReader, File>(_idxDir,new FileIndexableInterpreter(),
				new DefaultIndexReaderDecorator(), null, null, _batchSize, _batchDelay, true);
		System.out.println("Zoie system created for performance run");
	}
	
	public void startZoie() {
		_zoie.start();
		System.out.println("Zoie system started for performance run");
	}
	
	public void stopZoie() {
		// stop zoie system
		_zoie.stop();
		System.out.println("Zoie system stopped after performance run");
	}
	
	public void createProvider() {
	    _provider = new FileDataProvider(_idxInput);
	    _provider.setDataConsumer(_zoie);
	    _provider.setLooping(true);
	    if(_changeLoad) {
	    	changeIndexingLoad();
	    }
	}
	
	public synchronized void changeIndexingLoad() {
		if(!_throttleParams.empty()) {
			System.out.print("\nChanging throttle value from " + _throttle);
			_throttle = _throttleParams.pop();
			System.out.println(" to " + _throttle);
			_provider.setMaxEventsPerMinute(_throttle);
		}
	}

	public void startPerfRun(String perfRunDescription) {
		// start the perf thread
		_thread = new PerfThread(_opPerfFile, _zoie, perfRunDescription);
		_thread.start();
		System.out.println("Perf thread started for performance run");
		
		// start the data provider
		System.out.println("Data Provider started for performance run");
	    _provider.start();
	}
	
	public void stopPerfRun() {
		// stop the perf thread
		_thread.terminate();
		System.out.println("Server Perf thread stopped after performance run");
		
		// stop data provider
		_provider.stop();
		System.out.println("Data Provider stopped after performance run");
	}
	
	/**
	 * This function logs the indexing performance numbers in the opFile log file. No queries are being run on zoie during
	 * this time.
	 * @param batchSize
	 * @param batchDelay
	 * @param idxInput
	 * @param idxDir
	 * @param ipFile
	 * @param opFile
	 * @param runtime
	 */
	public static void indexingPerfNoQueryLoad(int batchSize, long batchDelay, File idxInput, File idxDir, 
			String ipFile, String opFile, long runtime, boolean changeLoad, boolean trackPerf) {
		MonitoredZoieSystem server = new MonitoredZoieSystem(batchSize, batchDelay, idxInput, idxDir, changeLoad, trackPerf);
		server.setIndexingIpPerfFile(ipFile);
		server.setIndexingOpPerfFile(opFile);
		
		// create the Zoie System
		server.createZoie();
		System.out.println("Created zoie system");
		
		// start the Zoie System
		server.startZoie();
		System.out.println("Zoie system started for performance run");

		// create the data provider
		server.createProvider();
		System.out.println("Created file data provider for " + idxDir.getAbsolutePath());
		
		// start the performance thread and the data provider
		server.startPerfRun("Indexing performance with no query load");
		System.out.println("Started perf run");

		// run performance server for specified "runtime"
		try {
			Thread.sleep(runtime);
		}catch(InterruptedException ie) {
			log.info("Finished waiting for " + runtime + "milliseconds");
			System.out.println("Finished waiting for " + runtime + "milliseconds");
		}

		// stop the perf run, flush to file
		server.stopPerfRun();
		
		// stop the zoie system
		server.stopZoie();
	}
	
	/**
	 * This function logs the indexing performance numbers in the opFile log file. During this time, increasing search load is
	 * exercised on the zoie system
	 * @param batchSize
	 * @param batchDelay
	 * @param idxInput
	 * @param idxDir
	 * @param ipFile
	 * @param opFile  the file containing the indexing performance numbers at the end of the performance run
	 * @param runtime duration over which the performance numbers should be pulled
	 */
	public static void indexingPerfWithQueryLoad(int batchSize, long batchDelay, File idxInput, File idxDir, 
			String ipFile, String opFile, long runtime, String queryfile, String waitfile, boolean changeIndexingLoad, boolean trackIndexingPerf) {
		MonitoredZoieSystem server = new MonitoredZoieSystem(batchSize, batchDelay, idxInput, idxDir, changeIndexingLoad, trackIndexingPerf);
		server.setIndexingIpPerfFile(ipFile);
		server.setIndexingOpPerfFile(opFile);

		// create the Zoie System
		server.createZoie();
		System.out.println("Created zoie system");
		
		// start the Zoie system
		server.startZoie();
		
		// create the data provider
		server.createProvider();
		System.out.println("Created file data provider for " + idxInput);

		// start query searcher threads
		ZoiePerfClient client = new ZoiePerfClient(server._zoie, true, false);
		client.loadQueries(queryfile);
		client.loadWaitTimings(waitfile);
		client.startPerfRun(null , null);
		
		// start indexing performance thread and the data provider
		server.startPerfRun("Indexing performance run with increasing query load");
		
		// run performance server for specified "runtime"
		try {
			Thread.sleep(runtime);
		}catch(InterruptedException ie) {
			log.info("Finished waiting for " + runtime + "milliseconds");
			System.out.println("Finished waiting for " + runtime + "milliseconds");
		}

		// stop the indexing performance run, flush to file
		server.stopPerfRun();	

		// stop client searcher threads
		client.endPerfRun();
		
		// stop the zoie system
		server.stopZoie();
	}
	
	/**
	 * This function logs the query performance numbers in the queryPerfFile log file. During this time, no indexing
	 * load is exercised on the system
	 * @param batchSize
	 * @param batchDelay
	 * @param idxInput
	 * @param idxDir
	 * @param queryFile		 file containing list of queries that should be run on the system
	 * @param waitFile		 file containing wait timings to throttle the search load on the system
	 * @param queryPerfFile  the file containing the query performance numbers at the end of the performance run
	 * @param runtime        duration over which the performance numbers should be pulled
	 */
	public static void queryPerfNoIndexingLoad(int batchSize, long batchDelay, File idxInput, File idxDir, 
			String queryFile, String waitFile, String queryPerfFile, long runtime,  boolean changeIndexingLoad, boolean trackIndexingPerf) {
		MonitoredZoieSystem server = new MonitoredZoieSystem(batchSize, batchDelay, idxInput, idxDir, changeIndexingLoad, trackIndexingPerf);

		// create the Zoie System
		server.createZoie();
		System.out.println("Created zoie system");
		
		// start the Zoie system
		server.startZoie();

		// start query searcher threads
		ZoiePerfClient client = new ZoiePerfClient(server._zoie, false, true);
		client.loadQueries(queryFile);
		client.loadWaitTimings(waitFile);
		System.out.println("Query performance output file-->" + queryPerfFile);
		client.startPerfRun("Query performance with no indexing load", queryPerfFile);

		// run client performance server for specified "runtime"
		System.out.println("Client performance run waiting for " + runtime + " milliseconds");
		try {
			Thread.sleep(runtime);
		}catch(InterruptedException ie) {
			log.info("Finished waiting for " + runtime + " milliseconds");
			System.out.println("Interrupt: Finished waiting for " + runtime + " milliseconds");
		}

		System.out.println("Finished waiting for " + runtime + " milliseconds");

		// stop client searcher threads
		client.endPerfRun();
		
		// stop the zoie system
		server.stopZoie();
	}

	/**
	 * This function logs the query performance numbers in the queryPerfFile log file. During this time, increasing indexing
	 * load is exercised on the system. Throttle parameters are read from the throttleFile
	 * @param throttleFile   file containing throttling parameters to vary indexing load on zoie
	 * @param batchSize
	 * @param batchDelay
	 * @param idxInput		 directory containing files to be indexed by zoie
	 * @param idxDir		 output directory where the index files are created
	 * @param queryFile		 file containing list of queries that should be run on the system
	 * @param waitFile		 file containing wait timings to throttle the search load on the system
	 * @param queryPerfFile  the file containing the query performance numbers at the end of the performance run
	 * @param runtime        duration over which the performance numbers should be pulled
	 */
	public static void queryPerfWithIndexingLoad(String throttleFile, int batchSize, long batchDelay, File idxInput, File idxDir, 
			String queryFile, String waitFile, String queryPerfFile, long runtime,  boolean changeIndexingLoad, boolean trackIndexingPerf) {
		MonitoredZoieSystem server = new MonitoredZoieSystem(batchSize, batchDelay, idxInput, idxDir, changeIndexingLoad, trackIndexingPerf);

		// create the Zoie System
		server.createZoie();
		System.out.println("Created zoie system");
		
		// start the Zoie system
		server.loadIndexingThrottle(throttleFile);
		server.startZoie();

		// create the data provider
		server.createProvider();
		System.out.println("Created file data provider for " + idxDir.getAbsolutePath());
		
		// start the performance thread and the data provider
		server.startPerfRun(null);

		// start query searcher threads
		ZoiePerfClient client = new ZoiePerfClient(server._zoie, false, true);
		client.loadQueries(queryFile);
		client.loadWaitTimings(waitFile);
		System.out.println("Query performance output file-->" + queryPerfFile);
		client.startPerfRun("Query performance with no indexing load", queryPerfFile);

		// run client performance server for specified "runtime"
		System.out.println("Client performance run waiting for " + runtime + " milliseconds");
		try {
			Thread.sleep(runtime);
		}catch(InterruptedException ie) {
			log.info("Finished waiting for " + runtime + " milliseconds");
			System.out.println("Interrupt: Finished waiting for " + runtime + " milliseconds");
		}

		System.out.println("Finished waiting for " + runtime + " milliseconds");

		// stop client searcher threads
		client.endPerfRun();
		
		// stop the zoie system
		server.stopZoie();
	}
	
	private static String now() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("MM.dd.yyyy 'at' hh:mm:ss z");
		return sdf.format(cal.getTime());
	}
	
	private final class PerfThread{
		private ZoieSystem<IndexReader, File> _zoieSystem;		
		private volatile AtomicLong _eventVersion = new AtomicLong(0);
		private volatile long _perfDuration = 1000;  // (millisecs) changing this will vary the indexing load
		private ArrayList<Long> _timings = new ArrayList<Long>(1000);
		private File _opFile;
		private String _perfRunDescription;
		private Timer _perfTimer;
		private Timer _throttleTimer;
		
		public PerfThread(String opFile, ZoieSystem<IndexReader, File> zoie, String perfRunDescription) {
			if(_trackIndexingPerf)
				_opFile = new File(opFile);
			_zoieSystem = zoie;
			_perfRunDescription = perfRunDescription;
		}
		
		/**
		 * @return the eventVersion
		 */
		public long getEventVersion() {
			return _eventVersion.get();
		}

		/**
		 * @param eventVersion the eventVersion to set
		 */
		public void setEventVersion(long eventVersion) {
			_eventVersion.set(eventVersion);
		}

		private void resetEventTimer() {
			_eventVersion.set(getNumEvents());
		}
		
		private long getNumEvents() {
			long diskVersion=0;
			try {
				diskVersion = _zoieSystem.getCurrentDiskVersion();
			}catch(IOException ioe) {
				log.error("Error reading disk version in perf thread" + ioe.getMessage());
			}
			long ramAVersion = _zoieSystem.getAdminMBean().getRamAVersion();
			long ramBVersion = _zoieSystem.getAdminMBean().getRamBVersion();
			return Math.max(Math.max(diskVersion, ramAVersion), ramBVersion);
		}
		
		public void start() {
			resetEventTimer();
			if(_trackIndexingPerf) {
				_perfTimer = new Timer();
				_perfTimer.schedule(new PerfTimer(), 0, _perfDuration);
			}
			if(_changeLoad) {
				System.out.println("\nScheduling throttle timer");
				_throttleTimer = new Timer();
				_throttleTimer.schedule(new ThrottleTimer(), 0, _throttleChangeDuration);
			}
		}

		void terminate()
		{
			if(_trackIndexingPerf) { 
				flush();
				_perfTimer.cancel();
				_perfTimer.purge();
			}
		}

		public void flush() {
			try {
				System.out.println("Flushing " + _timings.size() + " indexing timings to " + _opFile.getAbsolutePath());
				BufferedWriter bw = new BufferedWriter(new FileWriter(_opFile, true));
				bw.append(_perfRunDescription + " " + now() + "\n");
				for(Long timing: _timings) {
					bw.append(String.valueOf(timing));
					bw.append("\n");
				}
				bw.close();
			}catch(IOException ioe) {
				log.error("Unable to flush indexing performance numbers to perf file" + _opFile.getAbsolutePath() + ioe.getMessage());
			}
		}
		
		// timer to log indexing performance numbers
		private class PerfTimer extends TimerTask {
			private long oldVersion;
			private long timing;
			public void run() {
				oldVersion = getEventVersion();
				setEventVersion(getNumEvents());
//				System.out.println("Old version " + oldVersion + " And new version " + _eventVersion.get());
				// Log events per second
				timing = (getEventVersion() - oldVersion)*1000/(_perfDuration);
				System.out.println(" Events per second: " + timing);
				_timings.add(new Long(timing));
			}
		}
		
		// timer to increase indexing load
		private class ThrottleTimer extends TimerTask {
			public void run() {
				changeIndexingLoad();
			}
		}
}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// read the perf.properties file
		Properties properties = new Properties();
		System.out.println("Trying to load properties file");
		String propFile = "perf-new/conf/perf.properties";
		// TODO: Correct the logging
//	    String confdir = System.getProperty("zoie.conf.dir");
//	    org.apache.log4j.PropertyConfigurator.configure(confdir+"/log4j.properties");

		try {
			properties.load(new FileInputStream(propFile));
		}catch(IOException ioe) {
			System.err.println("Could not load properties file" + ioe.getMessage());
			log.error("Could not load perf.properties" + ioe.getMessage());
			System.exit(1);
		}

		System.out.println("Finished loading properties file");
		log.info("Finished loading properties file for performance testing");
		
		String ipFile = properties.getProperty("zoie.perf.ipfile");
		String opFile = properties.getProperty("zoie.perf.opfile");
		String batchSize = properties.getProperty("zoie.perf.batchSize");
		String batchDelay = properties.getProperty("zoie.perf.batchDelay");
		String idxDirPath = properties.getProperty("zoie.perf.idxdir");
		String runTime = properties.getProperty("zoie.perf.server.runtime");
		String indexInput = properties.getProperty("zoie.perf.input");
		String queryFile = properties.getProperty("zoie.client.queryfile");
		String waitFile = properties.getProperty("zoie.client.waittime");
		String queryPerfFile = properties.getProperty("zoie.client.perffile");
		String throttleFile = properties.getProperty("zoie.index.throttle");
		
		System.out.println("ipFile=" + ipFile + " opFile=" + opFile + " batchSize=" + batchSize + " batchDelay=" + batchDelay+
				" idxDirPath=" + idxDirPath + " Indexing input = " + indexInput + " Runtime = " + runTime + " Query performance output file = " + queryPerfFile);

		File idxDir = new File(idxDirPath);
		File idxInput = new File(indexInput);
		
		long runtime = 60000;       // run for a minute, default
		if(runTime != null) {
			runtime = Long.valueOf(runTime);
			// in perf.properties, runtime is specified in seconds
			runtime *= 1000;
		}
		
		// get indexing stats with no query load
//		indexingPerfNoQueryLoad(Integer.valueOf(batchSize).intValue(), Long.valueOf(batchDelay).longValue(),
//				idxInput, idxDir, ipFile, opFile, runtime, false, true);
//		
//		// delete the index directory for next run
//		try {
//			Runtime.getRuntime().exec("rm -rf " + idxDirPath);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.out.println("\nFailed to delete index for next indexing performance run");
//		}
//
//		System.out.println("\nIndex deleted for next indexing performance run");
//
//		// get indexing stats with increasing query load
//		indexingPerfWithQueryLoad(Integer.valueOf(batchSize).intValue(), Long.valueOf(batchDelay).longValue(),
//				idxInput, idxDir, ipFile, opFile, runtime, queryFile, waitFile, false, true);
//
//		// get query performance numbers with no indexing load
//		queryPerfNoIndexingLoad(Integer.valueOf(batchSize).intValue(), Long.valueOf(batchDelay).longValue(),
//				idxInput, idxDir, queryFile, waitFile, queryPerfFile, runtime, false, false);
		
		// get query performance numbers with increasing indexing load
		queryPerfWithIndexingLoad(throttleFile, Integer.valueOf(batchSize).intValue(), Long.valueOf(batchDelay).longValue(),
				idxInput, idxDir, queryFile, waitFile, queryPerfFile, runtime, true, false);
	}
}

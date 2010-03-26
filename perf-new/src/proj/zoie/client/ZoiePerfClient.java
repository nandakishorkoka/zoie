/**
 * 
 */
package src.proj.zoie.client;

import it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;

/**
 * @author nnarkhed
 *
 */
public class ZoiePerfClient<R extends IndexReader> {

	private static final Logger log = Logger.getLogger(ZoiePerfClient.class);

	private static final int SAMPLE_SIZE = 10000;

	private String[] _queries = new String[SAMPLE_SIZE];

	private int _numQueries = 0;

	private final IndexReaderFactory<R> _idxReaderFactory;		

	protected QueryDriver<R> _perfThread;

	protected Stack<Long> _waitTimings = new Stack<Long>();

	protected static int NANOS_IN_MILLI = 1000000;

	private String _perfRunDescription;

	private File _opFile;

	private boolean _changeQueryLoad;

	private boolean _trackQueryPerf;

	/**
	 * Constructor for query performance client
	 * @param idxReaderFactory	The backend zoie system
	 * @param changeQueryLoad	variable to track if the search load should be increased periodically or not
	 */
	public ZoiePerfClient(IndexReaderFactory<R> idxReaderFactory, boolean changeQueryLoad, boolean trackQueryPerf) {
		_idxReaderFactory = idxReaderFactory;
		_changeQueryLoad = changeQueryLoad;
		_trackQueryPerf = trackQueryPerf;
	}

	public void loadQueries(String queryfile) {
		File queryFile = new File(queryfile);
		BufferedReader br = null;
		try {
			br= new BufferedReader(new FileReader(queryFile));
		} catch (FileNotFoundException e) {
			log.error("Could not load queries file " + queryfile + "\n" + e.getMessage());
		}
		String line = null;
		_numQueries = 0;
		try {
			while((line = br.readLine()) != null) {
				_queries[_numQueries++] = line;
			}
		} catch (IOException e) {
			log.error("Error reading queries from file " + queryfile + "\n " + e.getMessage());
		}
		System.out.println("Loaded " + _numQueries + " queries from " + queryfile);
	}

	/**
	 * This function reads the wait timings for query load from the given file
	 * @param loadfile  the file containing the wait timings to vary the query load
	 */
	public void loadWaitTimings(String loadfile) {
		File queryFile = new File(loadfile);
		BufferedReader br = null;
		try {
			br= new BufferedReader(new FileReader(queryFile));
		} catch (FileNotFoundException e) {
			log.error("Could not load queries file " + loadfile + "\n" + e.getMessage());
		}
		String line = null;
		try {
			while((line = br.readLine()) != null) {
				_waitTimings.push(new Long(Long.valueOf(line)));
			}
		} catch (IOException e) {
			log.error("Error reading wait timings from file " + loadfile + "\n " + e.getMessage());
		}
		System.out.println("Loaded " + _waitTimings.size() + " wait timings from " + loadfile);
	}

	/**
	 * starts the client performance run
	 * @param perfRunDescription	The description for this performance run
	 * @param perfOpFile			The output file to which the query performance numbers should be flushed
	 */
	public void startPerfRun(String perfRunDescription, String perfOpFile) {
		_perfRunDescription = perfRunDescription;
		if(perfOpFile != null)
			_opFile = new File(perfOpFile);
		System.out.println("Starting query performance run..");
		_perfThread = new QueryDriver<R>(_opFile, _idxReaderFactory, _waitTimings);
		_perfThread.start();
	}

	/**
	 * ends the client performance run
	 */
	public void endPerfRun() {
		System.out.println("Client search threads stopping..");
		_perfThread.terminate();
		System.out.println("Client search threads stopped !!");
	}

	private static String now() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("MM.dd.yyyy 'at' hh:mm:ss z");
		return sdf.format(cal.getTime());
	}

	/**
	 * Query performance thread
	 */
	protected class QueryDriver<R extends IndexReader> extends Thread {
		private final IndexReaderFactory<R> _idxReaderFactory;		
		private static final int _numThreads = 50;
		private ScheduledExecutorService _threadPool;
		private static final long _waitTimeChangeDuration = 60000; // milliseconds
//		private static final long _waitTimeChangeDuration = 5000; // milliseconds
		protected volatile Stack<Long> _waitTimings = new Stack<Long>();
		private static final long _perfDuration = 1000; // milliseconds
		// numSearches value at the start of the performance window
		private volatile long _beginCount = 0L;
		// list of qps 
		private List<Long> _timings = new ArrayList<Long>(1000);
		// list of average query latencies 
		private List<Double> _latencyList = new ArrayList<Double>(1000);
		// tracks total latency of queries 
		private volatile long _latencySum = 0L;
		// tracks queries per second
		protected Long2IntMap _qps = new Long2IntLinkedOpenHashMap();
		// tracks total number of searches performed
		protected volatile AtomicInteger _numSearches = new AtomicInteger(0);
		// used for throttling queries, effectively varying the search load on zoie	
		private volatile long _waitTime;
		// timer thread to log performance numbers
		private Timer _perfTimer;
		// timer thread to change query load
		private Timer _queryLoadTimer;
		// variable used to stop the performance thread
		protected volatile boolean _perfRunStarted = false;
		// file to which performance numbers are flushed
		private File _opPerfFile;

		/**
		 * Sets the file to which performance numbers are flushed
		 * @param opPerfFile
		 */
		public QueryDriver(File opPerfFile, IndexReaderFactory<R> zoie, Stack<Long> waitTimings) {
			_opPerfFile = opPerfFile;
			_idxReaderFactory = zoie;
			_numSearches.set(0);
			_threadPool = Executors.newScheduledThreadPool(_numThreads);
			_waitTimings = waitTimings;
			// set the default wait timing
			_waitTime = _waitTimings.peek();
			System.out.println("Wait time is " + _waitTime);
		}

		/**
		 * varies the wait time to change query load
		 */
		public synchronized void changeWaitTime() {
			if(!_waitTimings.empty()) {
				System.out.print("\nChanging wait timing from " + _waitTime);
				_waitTime = _waitTimings.pop();
				System.out.println(" to " + _waitTime);
			}
		}

		public void start() {
			super.start();
			_perfRunStarted = true;
			reset();
			if(_trackQueryPerf) {
				_perfTimer = new Timer();
				_perfTimer.schedule(new PerfTimer(), 0, _perfDuration);
			}
			// change wait time duration only if query load is to be changed
			if(_changeQueryLoad) {
				_queryLoadTimer = new Timer();
				_queryLoadTimer.schedule(new QueryLoad(), 0, _waitTimeChangeDuration);
			}
		}

		public void terminate() {
			synchronized(this)
			{
				// flush the timings list to disk
				if(_opPerfFile != null)
					flush();
				_perfRunStarted = false;
				_threadPool.shutdownNow();
				// terminate the performance timer task
				if(_trackQueryPerf) {
					_perfTimer.cancel();
					_perfTimer.purge();
				}
				// terminate the query load changer timer task
				if(_changeQueryLoad) {
					_queryLoadTimer.cancel();
					_queryLoadTimer.purge();
				}
				this.notifyAll();
			}		
		}

		public void flush()  {
			try {
				//				System.out.println("Flushing query performance timings to " + _opPerfFile.getAbsolutePath());
				System.out.println("Flushing " + _timings.size() + " query performance timings");
				BufferedWriter bw = new BufferedWriter(new FileWriter(_opPerfFile, true));
				bw.append(_perfRunDescription + " " + now() + "\n");
				for(int i = 0;i < _latencyList.size();i ++) {
					bw.append("QueriesPerSec = " + String.valueOf(_timings.get(i)) + " Average Query Latency = " + _latencyList.get(i));
					bw.append("\n");
				}
				bw.close();
			}catch(IOException ioe) {
				log.error("Unable to flush query performance numbers to perf file" + _opFile.getAbsolutePath() + ioe.getMessage());
			}
		}

		/**
		 * reset the performance window
		 */
		private synchronized void reset() {
			_beginCount = _numSearches.get();
			_latencySum = 0L;
		}

		private class PerfTimer extends TimerTask {
			public void run() {
				System.out.println("\n\nQuery performance window reached ..");
				// number of queries performed in the last performance window
				long numQueries;
				synchronized(this) {
					numQueries = _numSearches.get() - _beginCount;
					if(numQueries > 0) {
//						long avgLatency = _latencySum/numQueries;
						double avgLatency = _latencySum/NANOS_IN_MILLI;
						System.out.println("Total query latency per sec : " + avgLatency + "\tAverage query latency : " + avgLatency/numQueries);
						_latencyList.add(new Double(avgLatency/numQueries));
						_latencySum = 0L;
					}
				}
				// track the queries/second
				long qps = (numQueries*1000)/_perfDuration;
				System.out.println("run() : _beginCount =  " + _beginCount + " and numQueries = "+ numQueries);
				System.out.println("Queries per second : " + qps);
				_timings.add(new Long(qps));
				if(_timings.size() >= 5000) {
					System.out.println("\n\n Performance cache almost full.. flushing to disk early..");
					flush();
					_timings.clear();
				}
				// reset the num searches
				reset();				
			}
		}

		private class QueryLoad extends TimerTask {
			public void run() {
				// increase the search load
				changeWaitTime();
			}
		}
		
		public void run() {
			while(_perfRunStarted) {
				synchronized(this) {
					if(_perfRunStarted) {
						// pick one query at random
						int i = (int) (_numQueries*Math.random());
						String query = _queries[i];
						final SearchRequest req = new SearchRequest();
						req.setQuery(query);
						try {
							// throttle the creation of search threads in the thread pool. If not done, could lead to out of heap space error
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						if(!_threadPool.isShutdown()) {
							_threadPool.scheduleAtFixedRate(
									new Runnable () {
										public void run() {
											try {
												search(req);
											} catch (ZoieException e) {
											}
										}
									}, 0, _waitTime, TimeUnit.MILLISECONDS);
						}
					}
				}
			}
		}

		/**
		 * performs a search based on the input SearchRequest 
		 * @param req	the search request
		 * @return		the search results
		 * @throws ZoieException
		 */
		public SearchResult search(SearchRequest req) throws ZoieException {
			if(_threadPool.isShutdown()) return null;
			if(req == null) throw new ZoieException("Search request is null");
			long start = System.nanoTime();
			//			System.out.println("Inside search() for query : " + req.getQuery());
			String queryString = req.getQuery();
			Analyzer analyzer = _idxReaderFactory.getAnalyzer();
			QueryParser qparser = new QueryParser(Version.LUCENE_CURRENT, "content", analyzer);

			SearchResult result = new SearchResult();
			List<R> readers = null;
			Searcher searcher = null;
			MultiReader multiReader = null;
			try {
				Query q = null;
				if(queryString == null || queryString.length() == 0) {
					q = new MatchAllDocsQuery();
				}
				else {
					q = qparser.parse(queryString);
				}
				readers = _idxReaderFactory.getIndexReaders();
				multiReader = new MultiReader(readers.toArray(new IndexReader[readers.size()]));
				searcher = new IndexSearcher(multiReader);
				TopDocs hits = searcher.search(q, 10);
				result.setTotalDocs(multiReader.numDocs());
				result.setTotalHits(hits.totalHits);	
				long end = System.nanoTime();
//				System.out.println("Latency in nanos --> " + (end-start));
//				long latency = (end-start)/NANOS_IN_MILLI;
				long latency = (end - start);
				result.setTime(latency);

				if(_trackQueryPerf) {
					synchronized(this) {
						_latencySum+=latency;
						_numSearches.incrementAndGet();
						//					System.out.println("\nI=search() : Increased the numSearches to " + _numSearches.get());
						//			log.info("search=[query=" + req.getQuery() + "]" + ", searchResult=[numSearchResults=" + result.getTotalHits() + ";numTotalDocs=" + result.getTotalDocs() + "]" + "in " + result.getTime() + "ms"); 
					} 
				}
				//				System.out.println("Query run : " + req.getQuery() + " Hit count : " + result.getTotalHits());
			}catch(Exception e) {
				log.error(e.getMessage(),e);
				throw new ZoieException(e.getMessage(), e);
			}finally {
				try {
					if(searcher != null) {
						searcher.close();
					}
				}catch(IOException ioe) {
					log.error(ioe.getMessage(), ioe);
				}finally {
					_idxReaderFactory.returnIndexReaders(readers);
				}
			}
			return result;
		}

	}
}

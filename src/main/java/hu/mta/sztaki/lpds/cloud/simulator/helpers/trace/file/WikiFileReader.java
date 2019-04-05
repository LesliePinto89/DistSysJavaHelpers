package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadLocalRandom;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

public class WikiFileReader extends TraceFileReaderFoundation {

	
	/**
	 * Constructs a "gwf" file reader that later on can act as a trace producer
	 * for user side schedulers.
	 * 
	 * @param fileName
	 *            The full path to the gwf file that should act as the source of
	 *            the jobs produced by this trace producer.
	 * @param from
	 *            The first job in the gwf file that should be produced in the
	 *            job listing output.
	 * @param to
	 *            The last job in the gwf file that should be still in the job
	 *            listing output.
	 * @param allowReadingFurther
	 *            If true the previously listed "to" parameter is ignored if the
	 *            "getJobs" function is called on this trace producer.
	 * @param jobType
	 *            The class of the job implementation that needs to be produced
	 *            by this particular trace producer.
	 * @throws SecurityException
	 *             If the class of the jobType cannot be accessed by the
	 *             classloader of the caller.
	 * @throws NoSuchMethodException
	 *             If the class of the jobType does not hold one of the expected
	 *             constructors.
	 */
	public WikiFileReader(String fileName, int from, int to, boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("Grid workload format", fileName, from, to, allowReadingFurther, jobType);
	}

	/**
	 * Determines if a particular line in the GWF file is representing a job
	 * 
	 * Actually ignores empty lines and lines starting with '#'
	 */
	@Override
	public boolean isTraceLine(final String line) {
		return basicTraceLineDetector("#", line);
	}

	/**
	 * Collects the total number of processors in the trace if specified in the
	 * comments
	 */
	@Override
	protected void metaDataCollector(String line) {
		if (line.contains("Processors")) {
			String[] splitLine = line.split("\\s");
			try {
				maxProcCount = parseLongNumber((splitLine[splitLine.length - 1]));
			} catch (NumberFormatException e) {
				// safe to ignore as there is no useful data here then
			}
		}
	}

	/**
	 * Parses a single line of the tracefile and instantiates a job object out
	 * of it.
	 * 
	 * Allows the creation of a job object using the GWA trace line format.
	 * 
	 * Supports GWA traces with millisecond time base (useful to load traces
	 * produced by the ASKALON workflow environment of University of Innsbruck).
	 *
	 * Not the entire GWF trace format is supported.
	 */
	@Override
	public Job createJobFromLine(String jobstring)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		boolean askalon = jobstring.endsWith("ASKALON");
		String[] elements = jobstring.trim().split("\\s+");
		
		if(elements[2].contains("error:unsupported-request-method")) {
			return null;
		}
		long jobState = Long.parseLong(elements[0]);
		int procs = ThreadLocalRandom.current().nextInt(1,4);  //Hard coded value for test purposes
		long runtime = 400;
		long waitTime = 0;
		String name = elements[2];
		
		String[] values = name.split("w");
		String desired = values[0];
		desired = desired.substring(0, desired.length()-1);
		//name = name.substring(0, endIndex)
				
		String submitTime = elements[1];
		submitTime = submitTime.substring(0, submitTime.length()-4);
		long submitToLong = Long.parseLong(submitTime);
				
		
		if (jobState != 1 && (procs < 1 || runtime < 0)) {
			return null;
		} else {
			return jobCreator.newInstance(
					// id
					elements[0],
					// submit time:
					
					
					submitToLong,
					// queueing time:
					Math.max(0, waitTime),
					// execution time:
					Math.max(0, runtime),
					// Number of processors
					Math.max(1, procs),
					// average execution time
					300,
					// no memory
					300,
					
					
					// User name:
					parseTextualField(desired),
					// Group membership:
					parseTextualField(desired),
					// executable name:
					parseTextualField(desired),
					// No preceding job
					null, 0);
		}
	}

	/**
	 * Checks if the particular GWA line entry contains useful data.
	 * 
	 * @param unparsed
	 *            the text to be checked for usefulness.
	 * @return the text altered after usefulness checking. If the text is not
	 *         useful then the string "N/A" is returned.
	 */
	private String parseTextualField(final String unparsed) {
		return unparsed.equals("-1") ? "N/A" : unparsed;
		// unparsed.matches("^-?[0-9](?:\\.[0-9])?$")?"N/A":unparsed;
	}

}


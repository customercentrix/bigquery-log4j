package webbarometer.tests;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import webbarometer.utils.BigqueryRefresher;
import webbarometer.utils.BigqueryUtils;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

/**
 * Unit test for verifying Log4j2 is sending log messages to BigQuery using the {@link BigQueryAppender} appender.
 * 
 * @author Sonny Trujillo
 */
public class Log4jTests
{
	static Logger log = LogManager.getLogger(Log4jTests.class);
	
	/**
	 * Logs DEBUG through FATAL error messages and verifies they are sent to BigQuery. 
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void loggingTest() throws InterruptedException
	{
		Bigquery bigquery = BigqueryUtils.makeBigqueryConnection(BigqueryUtils.SERVICE_ACCOUNT_EMAIL, new File("key.p12"));
		
		//Resets bigquery.
		new BigqueryRefresher().start();
		
		System.out.print("Logging.");
		log.debug("DEBUG Message");
		
		System.out.print(".");
		log.info("INFO Message");
		
		System.out.print(".");
		log.warn("WARN Message");
		
		System.out.print(".");
		log.error("ERROR Message");
		
		System.out.println(".");
		log.fatal("FATAL Message");
		
		System.out.println("Pausing for 30 seconds.");
		// Sleep four 30 seconds to allow bigquery to update
		Thread.sleep(30000);
		
		System.out.println("Verifying...");
		String querySql = "SELECT count(*) FROM [Logging.Log4jTestTable] LIMIT 1000";
		
		Job job = new Job();
		JobConfiguration config = new JobConfiguration();
		JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		config.setQuery(queryConfig);

		job.setConfiguration(config);
		queryConfig.setQuery(querySql);

		Insert insert;
		try
		{
			insert = bigquery.jobs().insert(BigqueryUtils.PROJECT_ID, job);		
			insert.setProjectId(BigqueryUtils.PROJECT_ID);
			JobReference jobId = insert.execute().getJobReference();
	
			System.out.println("Query Job is: "+jobId.getJobId());
									
			Job completedJob = pollJob(bigquery, jobId);
			verify(bigquery, completedJob);
		}
		catch (IOException e)
		{
			Assert.fail();
		}
		catch (InterruptedException e)
		{
			Assert.fail();
		}
	}
	
	/**
	 * Verifies the job completed and that there are 5 rows.
	 * 
	 * @param bigquery The {@link BigQuery}
	 * @param completedJob The completed {@link Job}
	 * 
	 * @throws IOException
	 */
	private void verify(Bigquery bigquery, Job completedJob) throws IOException
	{
		GetQueryResultsResponse queryResult = bigquery.jobs()
				.getQueryResults(
						BigqueryUtils.PROJECT_ID, completedJob
						.getJobReference()
						.getJobId()
				 ).execute();
				
		TableRow firstRow = queryResult.getRows().get(0);
		TableCell firstCell = firstRow.getF().get(0);
		
		Assert.assertTrue(firstCell.getV() + " != 5", "5".equals(firstCell.getV()));
		
		System.out.println("Success!");
	}
	
	/**
	 * Polls the job and returns once it is completed.
	 * 
	 * @param bigquery The bigQuery to use.
	 * @param jobId The jobs id
	 * @return The running {@link Job}.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private Job pollJob(Bigquery bigquery, JobReference jobId) throws IOException, InterruptedException
	{
		long startTime = System.currentTimeMillis();
		long elapsedTime = 0;

		while (true) 
		{
			Job pollJob = bigquery.jobs().get(BigqueryUtils.PROJECT_ID, jobId.getJobId()).execute();
			elapsedTime = System.currentTimeMillis() - startTime;
						
			if (pollJob.getStatus().getState().equals("DONE"))
				return pollJob;
			// Should only take 5 seconds, so we'll stop after 2 minutes.
			else if(elapsedTime > 120000)
				return null;
			
			
			// Pause execution for one second before polling job status again
			Thread.sleep(1000);
		}
	}
}

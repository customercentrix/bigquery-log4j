package webbarometer.appenders;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;

import webbarometer.utils.BigqueryUtils;

/**
 * Log4j2 Appender for sending log messages to Big Query
 * 
 * @author Sonny Trujillo <sonnyt@customercentrix.com>
 */
@Plugin(name = "BigQueryAppender", category = "Core", elementType = "appender", printObject = true)
public class BigQueryAppender extends AbstractAppender
{
	private static final String TEST_TABLE_NAME = "Log4jTestTable";
	
	private Bigquery bigquery;
	private TableReference testTableRef = new TableReference()
		.setProjectId(BigqueryUtils.PROJECT_ID)
		.setDatasetId(BigqueryUtils.DATASET_ID)
		.setTableId(TEST_TABLE_NAME);

	protected BigQueryAppender(String name, Filter filter, Layout<? extends Serializable> layout) throws GeneralSecurityException, IOException
	{
		super(name, filter, layout);
		
		bigquery = BigqueryUtils.makeBigqueryConnection(BigqueryUtils.SERVICE_ACCOUNT_EMAIL, new File("key.p12"));
	}

	@Override
	public void append(LogEvent event)
	{
		String thrown = event.getThrown() == null ? "" : event.getThrown().getMessage();
		String thread = event.getThreadName();
		String timestamp = BigqueryUtils.formatDate(new Date(event.getTimeMillis()));
		String level = event.getLevel().toString();
		String logger = event.getLoggerName();
		String message = event.getMessage().getFormattedMessage();

		TableRow row = new TableRow();		
		row.set("timestamp", timestamp);
		row.set("level", level);
		row.set("logger", logger);
		row.set("message", message);
		row.set("thread", thread);
		row.set("thrown", thrown);
		row.set("source", getSource());
		
		TableDataInsertAllRequest.Rows rows = new TableDataInsertAllRequest.Rows();
		rows.setJson(row);
		rows.setInsertId(timestamp);
				
		List<TableDataInsertAllRequest.Rows> data = new LinkedList<TableDataInsertAllRequest.Rows>();		
		data.add(rows);
		
		try
		{
			BigqueryUtils.insertTableData(bigquery, testTableRef, data);
		}
		catch (IOException e)
		{
			System.err.println("Could not insert table data into BigQuery: " + e.getMessage());			
		}				
	}

	/**
	 * Gets the Host Address and returns it.
	 * 
	 * @return The Host Address.
	 */
	private String getSource()
	{
		InetAddress ip;
		try 
		{
			ip = InetAddress.getLocalHost();
			return ip.getHostAddress();	 
		} 
		catch (UnknownHostException e) 
		{	 
			e.printStackTrace();	 
		}
		
		return null;
	}
	
	@PluginFactory
	public static BigQueryAppender createAppender(
			@PluginAttribute("name") String name,			
            @PluginElement("Filters") Filter filter,
			@PluginElement("Layout") Layout<? extends Serializable> layout) throws GeneralSecurityException, IOException 
	{
	    return new BigQueryAppender(name, filter, layout);
	}
	
}

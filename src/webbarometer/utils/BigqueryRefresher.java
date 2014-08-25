package webbarometer.utils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import webbarometer.utils.BigqueryUtils.FieldType;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Refreshes the Log4jTestTable by deleting it, then recreating it. 
 * 
 * @author Sonny Trujillo <sonnyt@customercentrix.com>
 */
public class BigqueryRefresher
{
	private static final String TEST_TABLE_NAME = "Log4jTestTable";
	
	private Bigquery bigquery;
	
	private TableReference tableReference;
	
	/**
	 * Connects to BigQuery, deletes the table, and then recreates it.
	 * 
	 * @param bigquery The {@link Bigquery}.
	 */
	public void start()
	{
		tableReference = new TableReference();
		tableReference.setProjectId(BigqueryUtils.PROJECT_ID);
		tableReference.setDatasetId(BigqueryUtils.DATASET_ID);
		tableReference.setTableId(TEST_TABLE_NAME);

		System.out.println("Connecting...");
		bigquery = BigqueryUtils.makeBigqueryConnection(BigqueryUtils.SERVICE_ACCOUNT_EMAIL, new File("key.p12"));
		
		System.out.println("Deleting "+TEST_TABLE_NAME+"...");
		deleteTestTable();
		
		System.out.println("Creating "+TEST_TABLE_NAME+"...");
		createTestTable();
		
		System.out.println("Done!");
	}
	
	/**
	 * Deletes the table defined in {@code tableReference}
	 */
	private void deleteTestTable()
	{
		try
		{			
			bigquery.tables().delete(tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId()).execute();			
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Creates the table defined in {@code tableReference} with the following schema:
	 * <li>timestamp : {@link FieldType#TIMESTAMP}</li>
	 * <li>level: {@link FieldType#STRING}</li>
	 * <li>logger: {@link FieldType#STRING}</li>
	 * <li>source: {@link FieldType#STRING}</li>
	 * <li>thread: {@link FieldType#STRING}</li>
	 * <li>thrown: {@link FieldType#STRING}</li>
	 * <li>message: {@link FieldType#STRING}</li>
	 */
	public void createTestTable()
	{
		List<TableFieldSchema> fields = new LinkedList<TableFieldSchema>();
		fields.add(BigqueryUtils.makeFieldSchema("timestamp", FieldType.TIMESTAMP));
		fields.add(BigqueryUtils.makeFieldSchema("level", FieldType.STRING));
		fields.add(BigqueryUtils.makeFieldSchema("logger", FieldType.STRING));
		fields.add(BigqueryUtils.makeFieldSchema("source", FieldType.STRING));
		fields.add(BigqueryUtils.makeFieldSchema("thread", FieldType.STRING));
		fields.add(BigqueryUtils.makeFieldSchema("thrown", FieldType.STRING));
		fields.add(BigqueryUtils.makeFieldSchema("message", FieldType.STRING));
		
		TableSchema schema = new TableSchema();
		schema.setFields(fields);
		
		BigqueryUtils.createAndInsertTable(bigquery, tableReference, schema);
	}
	
	public static void main(String[] args)
	{
		new BigqueryRefresher().start();
	}
	
}

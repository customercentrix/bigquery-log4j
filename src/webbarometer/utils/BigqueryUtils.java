package webbarometer.utils;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Tabledata.InsertAll;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * A utils class for Bigquery.
 * 
 * Most of the methods are wrapper methods for Bigquery operations that have multiple steps that all throw
 * exceptions.
 * 
 * @author Dan Green &lt;danielg@customercentrix.com&gt;
 * @since Jul 22, 2014
 */
public class BigqueryUtils
{
	public static final String PROJECT_ID = "useful-citizen-316";
	public static final String DATASET_ID = "Logging";
	public static final String SERVICE_ACCOUNT_EMAIL =
			"750903728331-t6hl208qt3v8r4e5bcr3ggi0ujkghc8k@developer.gserviceaccount.com";
	
	public static final int MAX_BATCH_SIZE = 500;
	
	private static final SimpleDateFormat bigqueryDateFormatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SS");	
	
	/**
	 * List all projects to which the authenticated account has at least read access.
	 * 
	 * @param bigquery
	 * 		The connected Bigquery object.
	 * @return
	 * 		A list containing the details of every project available to the authenticated account.
	 */
	public static List<ProjectList.Projects> listProjects(Bigquery bigquery)
	{
		try
		{
			return bigquery.projects().list().execute().getProjects();
		}
		catch(IOException e)
		{
			//TODO: Make this more detailed.
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * List all the datasets within a specified project.
	 * 
	 * @param bigquery
	 * @param projectId
	 * @return
	 */
	public static List<DatasetList.Datasets> listDatasets(Bigquery bigquery, String projectId)
	{
		try
		{
			return bigquery.datasets().list(projectId).execute().getDatasets();
		}
		catch(IOException e)
		{
			//TODO: Make this more detailed.
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * List all the tables within a specified dataset.
	 * 
	 * @param bigquery
	 * @param projectId
	 * @param datasetId
	 * @return
	 */
	public static List<TableList.Tables> listTables(Bigquery bigquery, String projectId, String datasetId)
	{
		try
		{
			return bigquery.tables().list(projectId, datasetId).execute().getTables();
		}
		catch(IOException e)
		{
			//TODO: Make this more detailed.
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static Dataset getDataset(Bigquery bigquery, String projectId, String datasetId)
	{
		try
		{
			return bigquery.datasets().get(projectId, datasetId).execute();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static Table getTable(Bigquery bigquery, TableReference ref)
	{
		try
		{
			return bigquery.tables().get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId()).execute();
		}
		catch(IOException e)
		{
			//TODO: Make this more detailed.
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static Table createAndInsertTable(Bigquery bigquery, TableReference ref, TableSchema schema)
	{
		Table table = new Table().setSchema(schema).setTableReference(ref);
		
		try
		{
			return bigquery.tables().insert(ref.getProjectId(), ref.getDatasetId(), table).execute();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * A convenience method for generating a field schema. Only takes required fields, all others set to default.
	 * 
	 * @param name
	 * 		The name of the field.
	 * @param type
	 * 		The type of field. If creating a {@link FieldType#RECORD}, use {@link #makeFieldSchema(String, List, FieldMode, String)}.
	 * @return
	 * 		The schema of the created field.
	 * 
	 * @see #makeFieldSchema(String, FieldType, FieldMode, String)
	 * @see #makeFieldSchema(String, List, FieldMode, String)
	 */
	public static TableFieldSchema makeFieldSchema(String name, FieldType type)
	{
		return makeFieldSchema(name, type, FieldMode.NULLABLE, null);
	}
	
	/**
	 * A convenience method for generating a field schema.
	 * 
	 * @param name
	 * 		The name of the field.
	 * @param type
	 * 		The type of field. If creating a {@link FieldType#RECORD}, use {@link #makeFieldSchema(String, List, FieldMode, String)}.
	 * @param mode
	 * 		The mode of this field.
	 * @param description
	 * 		This fields description. (Can be null)
	 * @return
	 * 		The schema of the created field.
	 * 
	 * @see #makeFieldSchema(String, FieldType)
	 * @see #makeFieldSchema(String, List, FieldMode, String)
	 */
	public static TableFieldSchema makeFieldSchema(String name, FieldType type, FieldMode mode, String description)
	{
		return new TableFieldSchema()
				.setName(name)
				.setType(type.toString())
				.setMode(mode.toString())
				.setDescription(description);
	}
	
	/**
	 * A convenience method for creating a field schema of the RECORD type.
	 * 
	 * @param name
	 * 		The name of the field.
	 * @param mode
	 * 		The mode of this field.
	 * @param description
	 * 		This fields description. (Can be null)
	 * @param nestedSchema
	 * 		The nested schema structure of this RECORD.
	 * @return
	 * 		The schema of the created field.
	 * 
	 * @see #makeFieldSchema(String, FieldType)
	 * @see #makeFieldSchema(String, FieldType, FieldMode, String)
	 */
	public static TableFieldSchema makeFieldSchema(String name, List<TableFieldSchema> nestedSchema, FieldMode mode, String description)
	{
		return new TableFieldSchema()
				.setName(name)
				.setType(FieldType.RECORD.toString())
				.setMode(mode.toString())
				.setDescription(description)
				.setFields(nestedSchema);
	}
	
	/**
	 * Inserts the given data into the given table.
	 * 
	 * @param bigquery
	 * @param ref
	 * @param data
	 * @return
	 */
	public static TableDataInsertAllResponse insertTableData(Bigquery bigquery, TableReference ref, List<TableDataInsertAllRequest.Rows> data)
	{
		TableDataInsertAllRequest insertRequest = new TableDataInsertAllRequest().setRows(data);
		
		try
		{
			InsertAll req = bigquery.tabledata().insertAll(PROJECT_ID, DATASET_ID, ref.getTableId(), insertRequest);			
			return req.execute();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * Format a date object in such a way that Bigquery can interpret it as a Timestamp. A
	 * Timestamp field only accepts Strings in the form "YYYY-MM-dd HH:mm[[:ss].SS]".
	 * 
	 * @param date
	 * 		The date to format as a String.
	 * @return
	 * 		A formatted string representing the given date.
	 */
	public static String formatDate(Date date)
	{
		return bigqueryDateFormatter.format(date);
	}
	
	/**
	 * Build a {@link GoogleCredential} that connects to Bigquery the provided Json parser and network connection.
	 * 
	 * @param serviceAccountEmail
	 * 		The google service email account that will be used for the connection.
	 * @param privateKey
	 * 		The private key for this account.
	 * @param jsonFactory
	 * 		The parser used in translating Json requests and responses to and from Bigquery.
	 * @param transport
	 * 		The HTTP Transport used as a connection.
	 * 
	 * @return
	 * 		A new {@link GoogleCredential} containing both the access and refresh tokens for accessing Bigquery.
	 * 
	 * @see #createBigqueryServiceCredentials(String, File)
	 * 
	 * @throws GeneralSecurityException
	 * @throws IOException
	 */
	public static GoogleCredential createBigqueryServiceCredentials(String serviceAccountEmail, File privateKey, 
			JsonFactory jsonFactory, HttpTransport transport) throws GeneralSecurityException, IOException
	{
		GoogleCredential credentials =
				new GoogleCredential.Builder().setJsonFactory(jsonFactory)
						.setTransport(transport)
						.setServiceAccountId(serviceAccountEmail)
						.setServiceAccountScopes(Arrays.asList(BigqueryScopes.BIGQUERY))
						.setServiceAccountPrivateKeyFromP12File(privateKey).build();
		
		return credentials;
	}
	
	/**
	 * Build a {@link GoogleCredential} that connects to Bigquery using the default JacksonFactory instance as a 
	 * Json parser and a new secure HttpTransport (using {@link GoogleNetHttpTransport#newTrustedTransport()}).
	 * 
	 * @param serviceAccountEmail
	 * 		The google service email account that will be used for the connection.
	 * @param privateKey
	 * 		The private key for this account.
	 * @return
	 * 		A new {@link GoogleCredential} containing both the access and refresh tokens for accessing Bigquery.
	 * 
	 * @see #createBigqueryServiceCredentials(String, File, JsonFactory, HttpTransport)
	 * 
	 * @throws GeneralSecurityException
	 * @throws IOException
	 */
	public static GoogleCredential createBigqueryServiceCredentials(String serviceAccountEmail, File privateKey) throws GeneralSecurityException, IOException
	{
		return createBigqueryServiceCredentials(serviceAccountEmail, privateKey, JacksonFactory.getDefaultInstance(), GoogleNetHttpTransport.newTrustedTransport());
	}
	
	public static Bigquery makeBigqueryConnection(String serviceAccountEmail, File privateKey)
	{
		try
		{
			JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
			HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
			
			GoogleCredential credentials = createBigqueryServiceCredentials(serviceAccountEmail, privateKey, jsonFactory, transport);
			return new Bigquery.Builder(transport, jsonFactory, credentials).build();
		}
		catch (GeneralSecurityException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * The possible values for a particular table field's "mode" value. If unspecified, a field will be
	 * set to NULLABLE.
	 * 
	 * @author Dan Green &lt;danielg@customercentrix.com&gt;
	 * @since Jul 22, 2014
	 */
	public enum FieldMode
	{
		/**
		 * Field CAN be set to null. This is the default behavior of fields.
		 */
		NULLABLE,
		/**
		 * Field CANNOT be set to null.
		 */
		REQUIRED,
		/**
		 * Indicates that this field can be set 0 or more times. For example,
		 * <pre>
		 * +- children: record (repeated)
         *          |  |- name: string
         *          |  |- gender: string
         *          |  |- age: integer
         * +- citiesLived: record (repeated)
         *          |  |- place: string
         *          |  +- yearsLived: integer (repeated)
         * </pre>
         * 
         * The above schema can support multiple records of children and multiple records of 
         * cities in a single table row. Each city record can also have multiples of the "yearsLived" field.<br /><br />
         * 
         * This field mode type is only supported if your table is stored in JSON format.<br /><br />
         * Also note that when querying tables with REPEATED data fields, the results will be automatically flattened.
		 */
		REPEATED;
	}
	
	/**
	 * The possible types of fields available in Bigquery. When specifying a {@link TableFieldSchema},
	 * a field type is required.
	 * 
	 * @author Dan Green &lt;danielg@customercentrix.com&gt;
	 * @since Jul 22, 2014
	 */
	public enum FieldType
	{
		STRING,
		INTEGER,
		FLOAT,
		BOOLEAN,
		/**
		 * A timestamp in the format "YYYY-MM-dd HH:mm:ss.SS". When setting this field, you must pass a String
		 * in that format. {@link BigqueryUtils} provides a convenience method for this called {@link BigqueryUtils#formatDate(Date)}.
		 */
		TIMESTAMP,
		/**
		 * A nested data structure. When specifying a field of the RECORD type, you must also 
		 * specify the structure of the record by calling {@link TableFieldSchema#setFields(List)}.
		 * A RECORD cannot be nested inside another RECORD.
		 */
		RECORD;
	}
}

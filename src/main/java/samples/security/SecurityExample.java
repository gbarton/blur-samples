package samples.security;

import static org.apache.blur.thrift.util.BlurThriftHelper.newRowMutation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.BlurConstants;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import samples.MakeTable;
import samples.queries.Sorting.ColumnSerializer;

/**
 * 
 * @author gman
 * 
 *         Table secureTable made that looks like this:
 * 
 *         <pre>
 *         row	record	family dataVal readAcl discoverAcl 
 *         1		1			fam1		val1		A			B
 *         1		2			fam2		val1		A			B
 * 
 *         2		1			fam1		val1		A&B		A|B
 *         2		2			fam2		val1		A&B		A|B
 * 
 * 			3		1			fam1		val1		B			A
 * 			3		2			fam2		val1		B			A
 * 
 * 			4		1			fam1		val1		-			-
 * 			4		2			fam2		val1		-			-
 * 
 * </pre>
 * 
 * 
 */
public class SecurityExample {

	public static final String TBL_NAME = "secureTable";
	public static final String FAM1 = "fam1";
	public static final String FAM2 = "fam2";
	public static final String COL1 = "col1";
	public static final String COL2 = "col2";

	/**
	 * make our standard record with 1 column, and secured with read and disc
	 * tags
	 * 
	 * @param fam
	 * @param read
	 * @param disc
	 * @return
	 */
	public static RecordMutation newRec(String fam, String recId, String read, String disc) {
		List<Column> cols = new ArrayList<Column>();
		cols.add(new Column(COL1, "val1"));
		cols.add(new Column(BlurConstants.ACL_READ, read));
		cols.add(new Column(BlurConstants.ACL_DISCOVER, disc));
		return new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, new Record(recId, fam,
				new ArrayList<Column>(cols)));
	}

	/**
	 * 
	 * @param query
	 *            lucene query
	 * @param rowQuery
	 *            is this a row query?
	 * @param read
	 *            csv readable tags
	 * @param disc
	 *            csv discoverable tags
	 * @param ids
	 *            csv rowid:recordId pairs to find ie: 1:1,2:1
	 * @throws TException 
	 * @throws BlurException 
	 * @throws JsonProcessingException 
	 */
	public static void query(Iface client, String query, boolean rowQuery, String read, String disc, String ids) throws BlurException, TException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(Column.class, new ColumnSerializer());
		mapper.registerModule(module);

		Query bQuery = new Query();
		bQuery.setQuery(query);
		bQuery.setRowQuery(rowQuery);

		Selector selector = new Selector();
		selector.setRecordOnly(!rowQuery);

		BlurQuery blurQuery = new BlurQuery();
		blurQuery.setFetch(10);
		blurQuery.setQuery(bQuery);
		blurQuery.setSelector(selector);

		// user attributes for setting acl's into
		Map<String, String> attributes = new HashMap<String, String>();

		//dont do this part!!
		String[] reads = read.split(",");
		for (String r : reads)
			attributes.put(BlurConstants.ACL_READ, r);
		String[] discs = disc.split(",");
		for (String d : discs)
			attributes.put(BlurConstants.ACL_DISCOVER, d);
		
		Set<String> idset = new TreeSet<String>();
		String[] keyStr = ids.split(",");
		for(String k : keyStr)
			idset.add(k);
			
		
		// set the user for this query
		User user = new User("readAUser", attributes);
		UserContext.setUser(user);

		BlurResults results = client.query(TBL_NAME, blurQuery);
		System.out.println(query + " rowQuery: " + rowQuery);
		System.out.println("Total Results: " + results.totalResults);
		
		Set<String> keys = new TreeSet<String>();
		for (BlurResult result : results.getResults()) {
			if(rowQuery) {
				 FetchRowResult rowResult = result.fetchResult.getRowResult();
				 String rowId = rowResult.row.id + ":";
				 for(Record r : rowResult.row.records)
					 keys.add(rowId + r.recordId);
				System.out.println(mapper.writeValueAsString(rowResult));
			} else {
				FetchRecordResult recordResult = result.getFetchResult().getRecordResult();
				keys.add(recordResult.rowid + ":" + recordResult.record.recordId);
				System.out.println(mapper.writeValueAsString(recordResult));
			}
		}
		
		System.out.println(idset);
		System.out.println(keys);
		System.out.println("Matched: " + keys.equals(idset));

		UserContext.reset();

	}

	public static void main(String[] args) throws BlurException, TException, JsonProcessingException {
		Iface client = BlurClient.getClient(args[0]);

		// check if table exists, bail if it does.

		if (!client.tableList().contains(TBL_NAME)) {

			// learn a clusters name
			List<String> clusterList = client.shardClusterList();
			String clusterName = "";
			if (clusterList.size() > 0)
				clusterName = clusterList.get(0);

			// force set a tmp location for working with if one doesnt exist

			// blur.hdfs.trace.path
			// blur.cluster.<cluster name here>.table.uri=<hdfs uri>

			final String tableURIProp = "blur.cluster." + clusterName + ".table.uri";
			String tableUri = client.configuration().get(tableURIProp);
			if (tableUri == null)
				tableUri = System.getProperty("java.io.tmpdir");

			tableUri += "/" + TBL_NAME;

			TableDescriptor td = new TableDescriptor().setCluster(clusterName).setName(TBL_NAME).setShardCount(2)
					.setTableUri(tableUri);

			// enables security checking on the table
			td.putToTableProperties(BlurConstants.BLUR_RECORD_SECURITY, "true");

			System.out.print("creating table.. ");
			client.createTable(td);
			System.out.println("created");

			// normal columns for each family
			client.addColumnDefinition(TBL_NAME, MakeTable.makeDef(FAM1, COL1, "string"));
			client.addColumnDefinition(TBL_NAME, MakeTable.makeDef(FAM2, COL1, "string"));

			// secure columns for each family
			client.addColumnDefinition(TBL_NAME,
					MakeTable.makeDef(FAM1, BlurConstants.ACL_READ, BlurConstants.ACL_READ));
			client.addColumnDefinition(TBL_NAME,
					MakeTable.makeDef(FAM1, BlurConstants.ACL_DISCOVER, BlurConstants.ACL_DISCOVER));
			client.addColumnDefinition(TBL_NAME,
					MakeTable.makeDef(FAM2, BlurConstants.ACL_READ, BlurConstants.ACL_READ));
			client.addColumnDefinition(TBL_NAME,
					MakeTable.makeDef(FAM2, BlurConstants.ACL_DISCOVER, BlurConstants.ACL_DISCOVER));

			System.out.println("table" + TBL_NAME + " created");
		}

		// add 3 rows.
		List<RowMutation> mutations = new ArrayList<RowMutation>();

		// row 1
		mutations.add(newRowMutation(RowMutationType.REPLACE_ROW, TBL_NAME, "1", newRec(FAM1, "1", "A", "B"),
				newRec(FAM2, "2", "A", "B")));

		// row 2
		mutations.add(newRowMutation(RowMutationType.REPLACE_ROW, TBL_NAME, "2", newRec(FAM1, "1", "A&B", "A|B"),
				newRec(FAM2, "2", "A&B", "A|B")));

		// row 3
		mutations.add(newRowMutation(RowMutationType.REPLACE_ROW, TBL_NAME, "3", newRec(FAM1, "1", "B", "A"),
				newRec(FAM2, "2", "B", "A")));

		// row 4
		mutations.add(newRowMutation(RowMutationType.REPLACE_ROW, TBL_NAME, "4", newRec(FAM1, "1", null, null),
				newRec(FAM2, "2", null, null)));

		client.mutateBatch(mutations);

		
		query(client, "+fam1.col1:val1", true, "A", "B", "1:1,1:2");
		query(client, "+fam1.col1:val1", false, "A", "B", "1:1,2:1");
		query(client, "+fam1.col1:val1", true, "A,B", "B", "1:1,1:2,2:1,2:2,3:1,3:2");
		
	}

}

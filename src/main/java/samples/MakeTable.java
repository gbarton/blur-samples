package samples;

import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.Blur.Iface;

/**
 * Creates a table for running samples against.
 * 
 * @author gman
 * 
 */
public class MakeTable {

	public static final String TBL_NAME = "tweets";
	public static final String TBL_FAM_SUMMARY = "summary";
	public static final String TBL_FAM_TWEET = "tweet";

	public static enum SUM_COLS {
		USER_ID, USER_NAME, DATE, YEAR, MONTH, DAY, TWEETS
	}

	public static enum TWEET_COLS {
		// link: URL within the text of the tweet
		LINK,
		// id: tweet id
		TWEET_ID,
		// create_at: date added to the db
		CREATE_AT,
		// create_at_long
		CREATE_AT_LONG,
		// inreplyto_screen_name: screen name of user this tweet is replying to
		INREPLY_TO_SN,
		// inreplyto_user_id: user id of user this tweet is replying to
		INREPLY_TO_USER_ID,
		// source: device from which the tweet originated
		SOURCE,
		// bad_user_id: alternate user id
		BAD_USER_ID,
		// user_screen_name: tweeting user screen name
		USER_SN,
		// order_of_users: tweet's index within sequence of tweets of the same
		// URL
		USER_ORDER,
		// user_id: user id
		USER_ID
	}

	public static void main(String[] args) throws BlurException, TException {
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

			System.out.print("creating table.. ");
			client.createTable(td);
			System.out.println("created");
		}

		// add a defined column definition

		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_TWEET, TWEET_COLS.LINK, "string"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_TWEET, TWEET_COLS.CREATE_AT_LONG, "long"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_TWEET, TWEET_COLS.TWEET_ID, "string"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_TWEET, TWEET_COLS.USER_ID, "string"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_TWEET, TWEET_COLS.USER_SN, "string"));

		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_SUMMARY, SUM_COLS.USER_ID, "string"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_SUMMARY, SUM_COLS.USER_NAME, "string"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_SUMMARY, SUM_COLS.DATE, "string"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_SUMMARY, SUM_COLS.DAY, "int"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_SUMMARY, SUM_COLS.MONTH, "int"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_SUMMARY, SUM_COLS.YEAR, "int"));
		client.addColumnDefinition(TBL_NAME, makeDef(TBL_FAM_SUMMARY, SUM_COLS.TWEETS, "int"));

	}

	public static ColumnDefinition makeDef(String fam, TWEET_COLS col, String type) {
		return makeDef(fam, col.toString(), type);
	}

	public static ColumnDefinition makeDef(String fam, SUM_COLS col, String type) {
		return makeDef(fam, col.toString(), type);
	}

	public static ColumnDefinition makeDef(String fam, String col, String type) {
		ColumnDefinition columnDefinition = new ColumnDefinition().setFamily(fam).setColumnName(col).setFieldType(type)
				.setSortable(true).setMultiValueField(false);
		System.out.println("Added column def: " + columnDefinition);
		return columnDefinition;
	}

}

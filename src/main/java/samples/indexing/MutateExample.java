package samples.indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;

import static org.apache.blur.thrift.util.BlurThriftHelper.*;
import samples.MakeTable.TWEET_COLS;
import static samples.MakeTable.*;

/**
 * example of mutate indexing of twitter data
 * 
 * @author gman
 * 
 */
public class MutateExample {

	public static class Summary {
		String userId;
		String userName;
		String date;
		int tweets = 1;

		public Summary(String u, String n, String d) {
			this.userId = u;
			this.userName = n;
			this.date = d;
		}

		public void add() {
			this.tweets++;
		}

		public String getRowId() {
			return this.userId + ":" + this.date;
		}

		public List<Column> getColumns() {
			List<Column> list = new ArrayList<Column>();

			list.add(new Column(SUM_COLS.USER_ID.toString(), userId));
			list.add(new Column(SUM_COLS.USER_NAME.toString(), userName));
			list.add(new Column(SUM_COLS.DATE.toString(), date.substring(0, 10)));
			list.add(new Column(SUM_COLS.YEAR.toString(), date.substring(0, 4)));
			list.add(new Column(SUM_COLS.MONTH.toString(), date.substring(5, 7)));
			list.add(new Column(SUM_COLS.DAY.toString(), date.substring(8, 10)));
			list.add(new Column(SUM_COLS.TWEETS.toString(), tweets + ""));

			return list;
		}

		@Override
		public String toString() {
			return "Summary [userId=" + userId + ", userName=" + userName + ", date=" + date + ", tweets=" + tweets
					+ "]";
		}

	}

	public static void loadTwitterData(File f, Iface client) throws Exception {

		TWEET_COLS[] values = TWEET_COLS.values();

		BufferedReader reader = new BufferedReader(new FileReader(f));

		long start = System.currentTimeMillis();
		System.out.println("starting load");

		Summary summary = null;
		String line = null;
		boolean header = true;
		int row = 0;
		RowMutation mut = null;

		List<RowMutation> mutations = new ArrayList<RowMutation>();
		int mutCount = 0;

		try {
			while ((line = reader.readLine()) != null) {
				// skip header
				if (header) {
					header = false;
					continue;
				}

				String[] splits = line.split(",");
				// rowid is the user_id:date
				String rowId = splits[10] + ":" + splits[2];

				// kickstart first record check
				if (summary == null) {
					summary = new Summary(splits[10], splits[8], splits[2]);
					// make new rowMutation
					mut = newRowMutation(RowMutationType.REPLACE_ROW, TBL_NAME, rowId, new RecordMutation(
							RecordMutationType.REPLACE_ENTIRE_RECORD, new Record(summary.userId, TBL_FAM_SUMMARY,
									summary.getColumns())));
				} else if (summary.getRowId().equals(rowId)) // same dude check
					summary.add();
				else {
					// new record write out old one.

//					RecordMutation recordMutation = new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, new Record(
//							summary.userId, TBL_FAM_SUMMARY, summary.getColumns()));
					
					
					// duplicative but it works.
					mut.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, new Record(
							summary.userId, TBL_FAM_SUMMARY, summary.getColumns())));

					mutations.add(mut);
					mutCount++;
					if (mutCount == 1000) {
//						client.mutateBatch(mutations);
						client.enqueueMutateBatch(mutations);
						mutations = new ArrayList<RowMutation>();
						mutCount = 0;
					}

					// make new rowMutation
					summary = new Summary(splits[10], splits[8], splits[2]);
					mut = newRowMutation(RowMutationType.REPLACE_ROW, TBL_NAME, splits[10] + ":" + splits[2],
							new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, new Record(summary.userId,
									TBL_FAM_SUMMARY, summary.getColumns())));

				}

				// do child tweet columns
				int i = 0;
				List<Column> cols = new ArrayList<Column>();

				for (TWEET_COLS t : values) {
					cols.add(new Column(t.toString(), splits[i++]));
				}

				mut.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, new Record(
						splits[1], TBL_FAM_TWEET, cols)));

				row++;
				if (row % 10000 == 0)
					System.out.println(row + " records @ " + (row / ((System.currentTimeMillis() - start) / 1000.0))
							+ " records/s");

			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("bah: " + line);
			throw e;
		}
		client.mutate(mut);

		reader.close();
		System.out.println("done");

	}

	public static void main(String[] args) throws Exception {
		Iface client = BlurClient.getClient(args[0]);

		loadTwitterData(new File(args[1]), client);
	}

}

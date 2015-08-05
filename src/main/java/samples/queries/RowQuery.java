package samples.queries;

import static samples.MakeTable.TBL_NAME;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.Blur.Iface;

import samples.queries.Sorting.ColumnSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class RowQuery {


	public static void query(Iface client) throws BlurException, TException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(Column.class, new ColumnSerializer());
		mapper.registerModule(module);

		Query query = new Query();
		query.setQuery("+tweet.USER_SN:zi*");
		query.setRowQuery(true);

		Selector selector = new Selector();
		selector.setRecordOnly(false);

		BlurQuery blurQuery = new BlurQuery();
		blurQuery.setFetch(1);
		blurQuery.setQuery(query);
		blurQuery.setSelector(selector);

		BlurResults results = client.query(TBL_NAME, blurQuery);
		System.out.println("Total Results: " + results.totalResults);
		for (BlurResult result : results.getResults()) {
			FetchRowResult rowResult = result.fetchResult.getRowResult();
			System.out.println(rowResult.totalRecords);
			rowResult.getRow().setRecords(null);
			System.out.println(mapper.writeValueAsString(rowResult));
		}
	}

	public static void main(String[] args) throws BlurException, TException, JsonProcessingException {

		Iface client = BlurClient.getClient(args[0]);
		// client.setUser(new User);

		query(client);
	}

}

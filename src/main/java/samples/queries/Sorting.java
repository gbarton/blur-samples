package samples.queries;

import java.io.IOException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.SortField;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import static samples.MakeTable.*;

/**
 * example of some sorted queries against our dataset
 * 
 * @author gman
 * 
 */
public class Sorting {

	public static class ColumnSerializer extends JsonSerializer<Column> {
	    @Override
	    public void serialize(Column col, JsonGenerator jgen, SerializerProvider provider) 
	      throws IOException, JsonProcessingException {
	        jgen.writeStartObject();
	        jgen.writeStringField("name", col.name);
	        jgen.writeStringField("value", col.value);
	        jgen.writeEndObject();
	    }
	}

	public static class FetchRecordSerializer extends JsonSerializer<FetchRecordResult> {
	    @Override
	    public void serialize(FetchRecordResult rec, JsonGenerator jgen, SerializerProvider provider) 
	      throws IOException, JsonProcessingException {
	        jgen.writeStartObject();
	        jgen.writeObjectField("record", rec.record);
	        jgen.writeStringField("rowid", rec.getRowid());
	        jgen.writeEndObject();
	    }
	}

	public static class RecordSerializer extends JsonSerializer<Record> {
	    @Override
	    public void serialize(Record rec, JsonGenerator jgen, SerializerProvider provider) 
	      throws IOException, JsonProcessingException {
	        jgen.writeStartObject();
	        jgen.writeObjectField("columns", rec.columns);
	        jgen.writeNumberField("columnsSize", rec.getColumnsSize());
	        jgen.writeStringField("family",rec.family);
	        jgen.writeEndObject();
	    }
	}

	
	public static void sort(Iface client) throws BlurException, TException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule();
		module.addSerializer(Column.class,new ColumnSerializer());
		mapper.registerModule(module);
		
		Query query = new Query();
		query.setQuery("+tweet.USER_SN:a*");
		query.setRowQuery(false);

		Selector selector = new Selector();
		selector.setRecordOnly(true);
		// we only want the family we are sorting on to come back
		selector.addToColumnFamiliesToFetch(TBL_FAM_TWEET);

		BlurQuery blurQuery = new BlurQuery();
		blurQuery.setFetch(10);
		blurQuery.setQuery(query);
		blurQuery.setSelector(selector);
		blurQuery.addToSortFields(new SortField(TBL_FAM_TWEET, TWEET_COLS.USER_SN.toString(), false));
		blurQuery.addToSortFields(new SortField(TBL_FAM_TWEET, TWEET_COLS.CREATE_AT_LONG.toString(), false));

		BlurResults results = client.query(TBL_NAME, blurQuery);
		System.out.println("Total Results: " + results.totalResults);
		for (BlurResult result : results.getResults()) {
			FetchRecordResult recordResult = result.fetchResult.getRecordResult();
			List<Column> columns = recordResult.getRecord().columns;
			System.out.println(mapper.writeValueAsString(recordResult));
//			print(columns, "CREATE_AT_LONG", "TWEET_ID", "USER_SN");
			// System.out.println(result);
		}
	}

	public static void print(List<Column> columns, String... cols) {
		for (String c : cols) {
			for (Column col : columns)
				if (col.getName().equals(c)) {
					System.out.print("\t" + c + ":" + col.getValue());
					break;
				}
		}
		System.out.println();
	}

	public static void main(String[] args) throws BlurException, TException, JsonProcessingException {

		Iface client = BlurClient.getClient(args[0]);
//		client.setUser(new User);

		sort(client);
	}

}

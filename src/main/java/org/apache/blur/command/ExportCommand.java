/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.command;

import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.command.annotation.RequiredArgument;
import org.apache.blur.command.commandtype.IndexReadCommandSingleTable;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.PrimeDocCache;
import org.apache.blur.manager.QueryParserUtil;
import org.apache.blur.server.TableContext;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.utils.BlurThriftRecord;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Exports data from a blur table into an HDFS location. This command will
 * export both a row and a record level query the same way, both end up just
 * writing records. Each record will end up being a single line in a txt file,
 * serialized into json. (Some formatting has occurred on the blur objects to
 * remove noise)
 * 
 * NOTE: the default blur object serialization thing doesnt work, so we
 * serialize our blurQuery into json using the provided serializeBlurQuery()
 * method.
 * 
 */
public class ExportCommand extends IndexReadCommandSingleTable<Long> {

	private static final Log LOG = LogFactory.getLog(ExportCommand.class);

	private BlurQuery blurQuery;

	@RequiredArgument("The BlurQuery serialized using serialize() into json format")
	private String blurQueryString;

	@RequiredArgument("The hdfs destination uri.  (e.g. hdfs://namenode/path)")
	private String destUri;

	@RequiredArgument("The hdfs user to run export command.")
	private String user;

	@Override
	public Long execute(final IndexContext context) throws IOException, InterruptedException {

		// get our blurQuery back
		blurQuery = mapper.readValue(blurQueryString, BlurQuery.class);

		final TableContext tableContext = context.getTableContext();
		final FieldManager fieldManager = tableContext.getFieldManager();
		final org.apache.blur.thrift.generated.Query simpleQuery = blurQuery.query;
		final boolean rowQuery = simpleQuery.rowQuery;
		final Term defaultPrimeDocTerm = tableContext.getDefaultPrimeDocTerm();
		// TODO: get filters working
		Filter queryFilter = null;
		// TODO: get columnFetch to work

		final ScoreType scoreType = ScoreType.CONSTANT;

		// have a query to run, setup file to output to:
		String shard = context.getShard().getShard();
		String uuid = blurQuery.uuid;
		final Path path = new Path(destUri, uuid + "-" + shard + ".json.gz");
		final byte[] newLine = new String("\n").getBytes();

		final AtomicLong exported = new AtomicLong(0);

		LOG.info("start shard: " + shard);

		UserGroupInformation remoteUser = UserGroupInformation.createRemoteUser(user);
		remoteUser.doAs(new PrivilegedExceptionAction<Long>() {

			public Long run() throws Exception {
				// setup query
				Query query;
				try {
					// query = parser.parse(queryStr);
					query = QueryParserUtil.parseQuery(simpleQuery.query, simpleQuery.rowQuery, fieldManager, null,
							null, scoreType, tableContext);
				} catch (ParseException e) {
					throw new IOException("query could not be parsed correctly", e);
				}

				// setup storage with existing conf
				FileSystem fs = FileSystem.get(tableContext.getConfiguration());
				final OutputStream outputStream = new GZIPOutputStream(fs.create(path, true));

				IndexSearcherCloseable indexSearcher = context.getIndexSearcher();
				indexSearcher.search(query, new Collector() {

					private AtomicReader _reader;
					private OpenBitSet _primeDocBitSet;
					private Bits _liveDocs;

					@Override
					public void collect(int doc) throws IOException {
						// doc equals primedoc in super query
						Row row = null;

						if (rowQuery) {
							int nextPrimeDoc = _primeDocBitSet.nextSetBit(doc + 1);
							for (int d = doc; d < nextPrimeDoc; d++) {
								// was our document marked for deletion?
								if (_liveDocs != null && !_liveDocs.get(d)) {
									continue;
								}
								Document document = _reader.document(d);
								BlurThriftRecord record = new BlurThriftRecord();
								String rowId = RowDocumentUtil.readRecord(document, record);
								row = new Row(rowId, record);
							}
						} else {
							Document document = _reader.document(doc);
							BlurThriftRecord record = new BlurThriftRecord();
							String rowId = RowDocumentUtil.readRecord(document, record);
							row = new Row(rowId, record);
						}
						// record has now been populated...
						String json = mapper.writeValueAsString(row);
						// LOG.info(json);
						outputStream.write(json.getBytes());
						outputStream.write(newLine);
						exported.incrementAndGet();
					}

					@Override
					public void setNextReader(AtomicReaderContext context) throws IOException {
						_reader = context.reader();
						_liveDocs = _reader.getLiveDocs();
						_primeDocBitSet = PrimeDocCache.getPrimeDocBitSet(defaultPrimeDocTerm, _reader);
					}

					@Override
					public void setScorer(Scorer scorer) throws IOException {

					}

					@Override
					public boolean acceptsDocsOutOfOrder() {
						return false;
					}
				});

				outputStream.flush();
				outputStream.close();
				// unused
				return exported.get();
			}
		});

		LOG.info("complete shard: " + shard + " exported: " + exported.get());
		return exported.get();
	}

	@Override
	public String getName() {
		return "ExportCommand";
	}

	public String getBlurQueryString() {
		return blurQueryString;
	}

	public void setBlurQueryString(String blurQueryString) {
		this.blurQueryString = blurQueryString;
	}

	public String getDestUri() {
		return destUri;
	}

	public void setDestUri(String destUri) {
		this.destUri = destUri;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	// SERIALIZATION CLASSES
	static final ObjectMapper mapper = new ObjectMapper();

	public static class Row {
		String rowId;
		BlurThriftRecord record;
		
		public Row() {}
		
		public Row(String rowId, BlurThriftRecord record) {
			this.rowId = rowId;
			this.record = record;
		}

		public String getRowId() {
			return rowId;
		}

		public void setRowId(String rowId) {
			this.rowId = rowId;
		}

		public BlurThriftRecord getRecord() {
			return record;
		}

		public void setRecord(BlurThriftRecord record) {
			this.record = record;
		}

	}

	static {
		SimpleModule module = new SimpleModule();
		module.addSerializer(Column.class, new ColumnSerializer());
		module.addSerializer(BlurThriftRecord.class, new RecordSerializer());
		module.addSerializer(org.apache.blur.thrift.generated.Query.class, new QuerySerializer());
		module.addSerializer(Selector.class, new SelectorSerializer());
		module.addSerializer(BlurQuery.class, new BlurQuerySerializer());
		mapper.registerModule(module);
	}

	public static class ColumnSerializer extends JsonSerializer<Column> {
		@Override
		public void serialize(Column col, JsonGenerator jgen, SerializerProvider provider) throws IOException,
				JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeStringField("name", col.name);
			jgen.writeStringField("value", col.value);
			jgen.writeEndObject();
		}
	}

	public static class BlurQuerySerializer extends JsonSerializer<BlurQuery> {
		@Override
		public void serialize(BlurQuery query, JsonGenerator jgen, SerializerProvider provider) throws IOException,
				JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeStringField("rowId", query.rowId);
			jgen.writeNumberField("maxQueryTime", query.maxQueryTime);
			jgen.writeNumberField("minimumNumberOfResults", query.minimumNumberOfResults);
			jgen.writeNumberField("fetch", query.fetch);
			jgen.writeNumberField("start", query.start);
			jgen.writeBooleanField("useCacheIfPresent", query.useCacheIfPresent);
			jgen.writeObjectField("selector", query.selector);
			jgen.writeObjectField("query", query.query);
			jgen.writeStringField("uuid", query.uuid);
			jgen.writeStringField("userContext", query.userContext);
			jgen.writeBooleanField("cacheResult", query.cacheResult);
			jgen.writeEndObject();
		}
	}

	public static class SelectorSerializer extends JsonSerializer<Selector> {
		@Override
		public void serialize(Selector sel, JsonGenerator jgen, SerializerProvider provider) throws IOException,
				JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeStringField("locationId", sel.locationId);
			jgen.writeStringField("recordId", sel.recordId);
			jgen.writeStringField("rowId", sel.rowId);
			writeArray(jgen, "columnFamiliesToFetch", sel.columnFamiliesToFetch);
			jgen.writeObjectField("columnsToFetch", sel.columnsToFetch);
			jgen.writeNumberField("maxRecordsToFetch", sel.maxRecordsToFetch);
			jgen.writeNumberField("startRecord", sel.startRecord);
			jgen.writeBooleanField("recordOnly", sel.recordOnly);
			jgen.writeEndObject();
		}
	}

	/**
	 * writes a set out, used for serialization
	 * 
	 * @param jgen
	 * @param field
	 * @param list
	 * @throws JsonGenerationException
	 * @throws IOException
	 */
	static void writeArray(JsonGenerator jgen, String field, Set<String> list) throws JsonGenerationException,
			IOException {
		if (list == null || list.size() == 0)
			return;
		jgen.writeArrayFieldStart(field);
		for (String item : list)
			jgen.writeString(item);
		jgen.writeEndArray();
	}

	public static class RecordSerializer extends JsonSerializer<BlurThriftRecord> {
		@Override
		public void serialize(BlurThriftRecord rec, JsonGenerator jgen, SerializerProvider provider)
				throws IOException, JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeObjectField("columns", rec.columns);
			jgen.writeNumberField("columnsSize", rec.getColumnsSize());
			jgen.writeStringField("family", rec.family);
			jgen.writeStringField("recordId", rec.recordId);
			jgen.writeEndObject();
		}
	}

	public static class QuerySerializer extends JsonSerializer<org.apache.blur.thrift.generated.Query> {
		@Override
		public void serialize(org.apache.blur.thrift.generated.Query query, JsonGenerator jgen,
				SerializerProvider provider) throws IOException, JsonProcessingException {
			jgen.writeStartObject();
			jgen.writeStringField("query", query.query);
			jgen.writeStringField("recordFilter", query.recordFilter);
			jgen.writeStringField("rowFilter", query.rowFilter);
			jgen.writeBooleanField("rowQuery", query.rowQuery);
			jgen.writeObjectField("scoreType", query.scoreType);
			jgen.writeEndObject();
		}
	}

	public static void main(String[] args) throws BlurException, TException, IOException {
		Iface client = BlurClient.getClient(args[0]);

		org.apache.blur.thrift.generated.Query query = new org.apache.blur.thrift.generated.Query();
		query.setQuery("+tweet.USER_SN:zin*");
		query.setRowQuery(true);

		Selector selector = new Selector();
		selector.setRecordOnly(false);

		BlurQuery blurQuery = new BlurQuery();
		blurQuery.setFetch(1);
		blurQuery.setQuery(query);
		blurQuery.setSelector(selector);
		blurQuery.setUuid(System.currentTimeMillis() + "");

		String qString = mapper.writeValueAsString(blurQuery);
		System.out.println(qString);

		BlurQuery q = mapper.readValue(qString, BlurQuery.class);

		ExportCommand export = new ExportCommand();
		export.setTable("tweets");
		export.setUser("testUser");
		export.setDestUri("/tmp/blurExport");
		export.setBlurQueryString(qString);
		// export.setBlurQuery(blurQuery);
		Map<Shard, Long> run = export.run(client);
		for (Shard s : run.keySet())
			System.out.println(s.getShard() + ": " + run.get(s));
	}

}

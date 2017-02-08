/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.elasticsearch2;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sink that emits its input elements in bulk to an Elasticsearch cluster.
 *
 * <p>
 * When using the second constructor
 * {@link #ElasticsearchSink(java.util.Map, java.util.List, ElasticsearchSinkFunction)} a {@link TransportClient} will
 * be used.
 *
 * <p>
 * <b>Attention: </b> When using the {@code TransportClient} the sink will fail if no cluster
 * can be connected to.
 *
 * <p>
 * The {@link Map} passed to the constructor is forwarded to Elasticsearch when creating
 * {@link TransportClient}. The config keys can be found in the Elasticsearch
 * documentation. An important setting is {@code cluster.name}, this should be set to the name
 * of the cluster that the sink should emit to.
 *
 * <p>
 * Internally, the sink will use a {@link BulkProcessor} to send {@link IndexRequest IndexRequests}.
 * This will buffer elements before sending a request to the cluster. The behaviour of the
 * {@code BulkProcessor} can be configured using these config keys:
 * <ul>
 *   <li> {@code bulk.flush.max.actions}: Maximum amount of elements to buffer
 *   <li> {@code bulk.flush.max.size.mb}: Maximum amount of data (in megabytes) to buffer
 *   <li> {@code bulk.flush.interval.ms}: Interval at which to flush data regardless of the other two
 *   settings in milliseconds
 * </ul>
 *
 * <p>
 * You also have to provide an {@link RequestIndexer}. This is used to create an
 * {@link IndexRequest} from an element that needs to be added to Elasticsearch. See
 * {@link RequestIndexer} for an example.
 *
 * @param <T> Type of the elements emitted by this sink
 */
public class ElasticsearchSink<T> extends RichSinkFunction<T>  {

	public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
	public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
	public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";
	public static final String CONFIG_KEY_CLIENT_PING_TIMEOUT_MS = "client.transport.ping_timeout";
	
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

	/**
	 * The user specified config map that we forward to Elasticsearch when we create the Client.
	 */
	private final Map<String, String> userConfig;

	/**
	 * The list of nodes that the TransportClient should connect to. This is null if we are using
	 * an embedded Node to get a Client.
	 */
	private final List<InetSocketAddress> transportAddresses;

	/**
	 * The builder that is used to construct an {@link IndexRequest} from the incoming element.
	 */
	private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

	/**
	 * The Client that was either retrieved from a Node or is a TransportClient.
	 */
	private transient Client client;

	/**
	 * Bulk processor that was created using the client
	 */
	private transient BulkProcessor bulkProcessor;

	/**
	 * Bulk {@link org.elasticsearch.action.ActionRequest} indexer
	 */
	private transient RequestIndexer requestIndexer;

	/**
	 * This is set from inside the BulkProcessor listener if a Throwable was thrown during processing.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();
	
	/**
	 * When set to <code>true</code> and the bulk action fails, the error message will be checked for
	 * common patterns like <i>timeout</i>, <i>UnavailableShardsException</i> or a full buffer queue on the node.
	 * When a matching pattern is found, the bulk will be retried.
	 */
	protected boolean checkErrorAndRetryBulk = false;

	/**
	 * Creates a new ElasticsearchSink that connects to the cluster using a TransportClient.
	 *
	 * @param userConfig The map of user settings that are passed when constructing the TransportClient and BulkProcessor
	 * @param transportAddresses The Elasticsearch Nodes to which to connect using a {@code TransportClient}
	 * @param elasticsearchSinkFunction This is used to generate the ActionRequest from the incoming element
	 *
	 */
	public ElasticsearchSink(Map<String, String> userConfig, List<InetSocketAddress> transportAddresses, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		if (!userConfig.containsKey(CONFIG_KEY_CLIENT_PING_TIMEOUT_MS)) {
			// Rise timeout to avoid ReceiveTimeoutTransportException request_id [5502798] timed out after [5000ms]]
			userConfig.put(CONFIG_KEY_CLIENT_PING_TIMEOUT_MS, "30s");
		}
		
		this.userConfig = userConfig;
		this.elasticsearchSinkFunction = elasticsearchSinkFunction;
		Preconditions.checkArgument(transportAddresses != null && transportAddresses.size() > 0);
		this.transportAddresses = transportAddresses;
	}

	/**
	 * Initializes the connection to Elasticsearch by creating a
	 * {@link org.elasticsearch.client.transport.TransportClient}.
	 */
	@Override
	public void open(Configuration configuration) {
		List<TransportAddress> transportNodes;
		transportNodes = new ArrayList<>(transportAddresses.size());
		for (InetSocketAddress address : transportAddresses) {
			transportNodes.add(new InetSocketTransportAddress(address));
		}

		Settings settings = Settings.settingsBuilder().put(userConfig).build();

		TransportClient transportClient = TransportClient.builder().settings(settings).build();
		for (TransportAddress transport: transportNodes) {
			transportClient.addTransportAddress(transport);
		}

		// verify that we actually are connected to a cluster
		ImmutableList<DiscoveryNode> nodes = ImmutableList.copyOf(transportClient.connectedNodes());
		if (nodes.isEmpty()) {
			throw new RuntimeException("Client is not connected to any Elasticsearch nodes!");
		}

		client = transportClient;

		if (LOG.isInfoEnabled()) {
			LOG.info("Created Elasticsearch TransportClient {}", client);
		}

		BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {

			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				if (response.hasFailures()) { System.out.println("Failed");
					BulkItemResponse itemResp;
					for (int i = 0; i < response.getItems().length; i++) {
						itemResp = response.getItems()[i];
						if (itemResp.isFailed()) {
							if (checkErrorAndRetryBulk && isRetryableCause(itemResp.getFailure().getCause())) { // Check if index request can be retried
								LOG.debug("Retry request: {}", itemResp.getFailureMessage());
								
								ActionRequest<?> req = request.requests().get(i);
								
								// There is no waiting time between index requests, so this may produce additional pressure on cluster
								bulkProcessor.add(req);
								
							} else { // Cannot retry action
								LOG.error("Failed to index document in Elasticsearch: {}", itemResp.getFailureMessage());
								failureThrowable.compareAndSet(null, new RuntimeException(itemResp.getFailureMessage()));	
							}
						}
					}
				}
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				if (checkErrorAndRetryBulk && isRetryableCause(failure)) {
					LOG.debug("Retry bulk on throwable: {}", failure.getMessage());

					for (ActionRequest<?> req : request.requests()) {
						// There is no waiting time between index requests, so this may produce additional pressure on cluster
						bulkProcessor.add(req);
					}
				} else { 
					LOG.error("Failed to index bulk in Elasticsearch. {}", failure.getMessage());
					failureThrowable.compareAndSet(null, failure);
				}
			}
		});

		// This makes flush() blocking
		bulkProcessorBuilder.setConcurrentRequests(0);

		ParameterTool params = ParameterTool.fromMap(userConfig);

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
			bulkProcessorBuilder.setBulkActions(params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS));
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
			bulkProcessorBuilder.setBulkSize(new ByteSizeValue(params.getInt(
					CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB), ByteSizeUnit.MB));
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
			bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(params.getInt(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)));
		}

		bulkProcessor = bulkProcessorBuilder.build();
		requestIndexer = new BulkProcessorIndexer(bulkProcessor);
	}
	
	/**
	 * Check if the cause is only temporary and thus retryable
	 */
	public static boolean isRetryableCause(Throwable throwable) {
		boolean retry =
				ExceptionUtils.indexOfThrowable(throwable, EsRejectedExecutionException.class) >= 0 || // Bulk index queue on node full*/
				ExceptionUtils.indexOfThrowable(throwable, ElasticsearchTimeoutException.class) >= 0 || // ElasticsearchTimeoutException sounded good, not seen in stress tests yet
				ExceptionUtils.indexOfThrowable(throwable, UnavailableShardsException.class) >= 0 || // Shard not available due to rebalancing or node down
				ExceptionUtils.indexOfThrowable(throwable, ClusterBlockException.class) >= 0 || // Examples: "no master"
				ExceptionUtils.indexOfThrowable(throwable, NodeNotConnectedException.class) >= 0; // Timeout from node and node has not reconnected yet
				
		//LOG.warn("Will "+ (retry ? "" : "not") +" retry", throwable);
		return retry;
	}
	
	/**
	 * Tells if a failed bulk request will be retried on certain error messages.
	 * @return
	 */
	public boolean getCheckErrorAndRetryBulk() {
		return checkErrorAndRetryBulk;
	}

	/**
	 * Set to <code>true</code> if the bulk request should be retried on certain error messages.
	 * @param checkErrorAndRetryBulk
	 */
	public void setCheckErrorAndRetryBulk(boolean checkErrorAndRetryBulk) {
		this.checkErrorAndRetryBulk = checkErrorAndRetryBulk;
	}

	@Override
	public void invoke(T element) {
		elasticsearchSinkFunction.process(element, getRuntimeContext(), requestIndexer);
	}

	@Override
	public void close() {
		if (bulkProcessor != null) {
			bulkProcessor.close();
			bulkProcessor = null;
		}

		if (client != null) {
			client.close();
		}

		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occured in ElasticsearchSink.", cause);
		} else {
			throw new RuntimeException("An error occured in ElasticsearchSink.");
		}
	}

}

package io.markreddy;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 9300);
        transportAddresses.add(inetSocketAddress);

        Map<String, String> config = new HashMap<>();
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("cluster.name", parameterTool.getRequired("es-cluster.name"));


        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer09<>(
                        parameterTool.getRequired("kakfa-topic"),
                        new SimpleStringSchema(),
                        parameterTool.getProperties()));

        messageStream.rebalance().addSink(new ElasticsearchSink<>(config, transportAddresses, getEsSinkFunction()));

        env.execute("Kafka to ES Stream");

    }

    private static ElasticsearchSinkFunction<String> getEsSinkFunction() {
        return new ElasticsearchSinkFunction<String>() {
            public IndexRequest createIndexRequest(String element) {
                Map<String, String> json = new HashMap<>();
                json.put("data", element);

                return Requests.indexRequest()
                        .index("test-index")
                        .type("test-type")
                        .source(json);

            }

            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        };
    }
}


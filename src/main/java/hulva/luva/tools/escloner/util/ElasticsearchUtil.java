package hulva.luva.tools.escloner.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.client.transport.TransportClient.Builder;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hulva.luva.tools.escloner.ESCloner;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年3月18日
 * @description
 *
 */
public class ElasticsearchUtil {
    protected final static Logger LOGGER = LoggerFactory.getLogger(ElasticsearchUtil.class);

    private static TransportClient sourceClient = null;
    private static TransportClient destinationClient = null;


    public static TransportClient sourceClient() {
        return sourceClient;
    }

    public static TransportClient destinationClient() {
        return destinationClient;
    }


    /**
     * initialize <code>TransportClient</code> for source server and destination server
     * 
     * @param sourceUrls e.g. <code>host1:9300,host2:9300</code>
     * @param destinationUrls e.g. <code>host1:9300,host2:9300</code>
     * @throws NumberFormatException
     * @throws UnknownHostException
     */
    public static void init(String sourceUrls, String destinationUrls)
            throws NumberFormatException, UnknownHostException {

        Settings settings = Settings.builder().put("client.transport.sniff", true)
                .put("client.transport.ignore_cluster_name", true).build();
        sourceClient = new Builder().settings(settings).build().addTransportAddresses(
                generateTransportAddress(sourceUrls).toArray(new TransportAddress[0]));

        destinationClient = new Builder().settings(settings).build().addTransportAddresses(
                generateTransportAddress(destinationUrls).toArray(new TransportAddress[0]));
    }

    /**
     * generate Sting <em>host1:9300,host2:9300</em> to <code>Set<TransportAddress></code>
     * 
     * @param urls
     * @return Set<TransportAddress>
     * @throws NumberFormatException
     * @throws UnknownHostException
     */
    private static Set<TransportAddress> generateTransportAddress(String urls)
            throws NumberFormatException, UnknownHostException {
        String[] arrUrl = urls.split(",");
        Set<TransportAddress> transportAddresses = new HashSet<TransportAddress>();
        String[] splitted = null;
        for (String url : arrUrl) {
            splitted = url.split(":");
            transportAddresses.add(new InetSocketTransportAddress(
                    InetAddress.getByName(splitted[0]), Integer.parseInt(splitted[1])));
        }
        return transportAddresses;
    }

    public static void matchAllQuery(TransportClient client, String index)
            throws InterruptedException {

        SearchResponse scrollResp = client.prepareSearch(index)
                // .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery())
                .setSize(500).execute().actionGet(); // 100 hits per shard will be returned for each
                                                     // scroll
        // Scroll until no hits are returned
        BulkRequestBuilder bulkRequest = null;
        TransportClient desClient = ElasticsearchUtil.destinationClient();
        while (true) {

            bulkRequest = desClient.prepareBulk();
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                bulkRequest.add(desClient.prepareIndex(hit.getIndex(), hit.getType(), hit.getId())
                        .setSource(hit.getSource()));

            }

            ESCloner.searchHitCache.put(bulkRequest);

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000)).execute().actionGet();
            // Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                ESCloner.done.set(true);
                LOGGER.info("Query done!");
                break;
            }
        }
    }


    public static void bulkIndexDocument(TransportClient client, BulkRequestBuilder bulkRequest) {

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // retry once
            BulkResponse retryBulkResponse = bulkRequest.get();
            if (retryBulkResponse.hasFailures()) {
                throw new RuntimeException(
                        "bulkIndexDocument failed! " + retryBulkResponse.buildFailureMessage());
            }
        }
    }


    public static boolean isIndexTemplateExists(TransportClient client, String templateName) {
        ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String, IndexTemplateMetaData> templates =
                resp.getState().metaData().templates();
        if (templates.containsKey(templateName)) {
            return true;
        }
        return false;
    }

    public static IndexTemplateMetaData getIndexTemplate(TransportClient client,
            String templateName) {
        ClusterStateResponse resp = client.admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String, IndexTemplateMetaData> templates =
                resp.getState().metaData().templates();
        if (templates.containsKey(templateName)) {
            return templates.get(templateName);
        }
        return null;
    }

    public static void putIndexTemplate(TransportClient client, String templateName,
            String template) {
        // TODO If it is possible to know what exactly the template has been changed, it'll be more
        // efficiently while update a index template. For now, i just update the whole content.
        // 20170318
        // Hulva Luva.H from ECBD

        client.admin().indices().preparePutTemplate(templateName).setSource(template).get();
    }

}

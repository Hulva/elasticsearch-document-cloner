package hulva.luva.tools.escloner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import hulva.luva.tools.escloner.util.CommandLineUtil;
import hulva.luva.tools.escloner.util.ElasticsearchUtil;

public class ESCloner {

    protected final static Logger LOGGER = LoggerFactory.getLogger(ESCloner.class);

    public static BlockingQueue<BulkRequestBuilder> searchHitCache;
    public static ExecutorService putterPool;
    public static AtomicBoolean done = new AtomicBoolean(false);
    public static AtomicInteger count = new AtomicInteger();

    public static void main(String[] args) throws Exception {

        CommandLine cmd = CommandLineUtil.readCommandLine(args);

        String sourceServer = cmd.getOptionValue("sServer");
        String destinationServer = cmd.getOptionValue("dServer");
        String index = cmd.getOptionValue("index");
        String template = cmd.getOptionValue("template");

        ElasticsearchUtil.init(sourceServer, destinationServer);

        searchHitCache = new LinkedBlockingQueue<BulkRequestBuilder>(50);

        IndexTemplateMetaData indexTemplateMatadata =
                ElasticsearchUtil.getIndexTemplate(ElasticsearchUtil.sourceClient(), template);

        if (indexTemplateMatadata == null) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar escloner.jar", CommandLineUtil.createOptions());
            System.err.println("template ->" + template + "<- not exists in source elasticsearch!");
            System.exit(0);
        }

        if (!ElasticsearchUtil.isIndexTemplateExists(ElasticsearchUtil.destinationClient(),
                template)) {
            ElasticsearchUtil.putIndexTemplate(ElasticsearchUtil.destinationClient(), template,
                    generateTemplate(indexTemplateMatadata));
        }

        int poolSize = Runtime.getRuntime().availableProcessors();
        /*
         * if (cmd.hasOption("pSize")) { poolSize = Integer.valueOf(cmd.getOptionValue("pSize")); }
         */
        putterPool = Executors.newFixedThreadPool(poolSize);

        for (int i = 0; i < poolSize; i++) {
            putterPool.submit(new ElasticsearchDocumentPutter());
        }

        ElasticsearchUtil.matchAllQuery(ElasticsearchUtil.sourceClient(), index);

        putterPool.shutdown();

    }

    private static String generateTemplate(IndexTemplateMetaData indexTemplateMatadata)
            throws JSONException, IOException {
        JSONObject templateJson = new JSONObject();
        JSONObject mappings = new JSONObject();
        Iterator<ObjectObjectCursor<String, CompressedXContent>> iterator =
                indexTemplateMatadata.getMappings().iterator();
        ObjectObjectCursor<String, CompressedXContent> mapping = null;
        JSONObject temp = null;
        while (iterator.hasNext()) {
            mapping = iterator.next();
            temp = new JSONObject(mapping.value.string());
            mappings.put(mapping.key, temp.get(mapping.key));
        }

        ImmutableOpenMap<String, AliasMetaData> aliasesMap = indexTemplateMatadata.getAliases();
        if (aliasesMap.isEmpty()) {
            templateJson.put("aliases", new HashMap<String, AliasMetaData>(0));
        }

        templateJson.put("template", indexTemplateMatadata.getTemplate())
                .put("settings", indexTemplateMatadata.getSettings().getAsMap())
                .put("mappings", mappings);
        LOGGER.info(templateJson.toString());
        return templateJson.toString();
    }
}

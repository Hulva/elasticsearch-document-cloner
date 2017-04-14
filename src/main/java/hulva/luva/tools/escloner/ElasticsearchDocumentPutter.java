package hulva.luva.tools.escloner;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hulva.luva.tools.escloner.util.ElasticsearchUtil;

/**
 * @author Hulva Luva.H from ECBD
 * @date 2017年4月13日
 * @description
 *
 */
public class ElasticsearchDocumentPutter implements Runnable {
    protected final static Logger LOGGER =
            LoggerFactory.getLogger(ElasticsearchDocumentPutter.class);

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        try {
            BulkRequestBuilder bulkRequest = null;
            while (true) {
                if (ESCloner.done.get() && ESCloner.searchHitCache.isEmpty()) {
                    LOGGER.info(
                            "PUT documents thread-" + Thread.currentThread().getName() + "done!");
                    return;
                }
                bulkRequest = ESCloner.searchHitCache.take();
                ElasticsearchUtil.bulkIndexDocument(ElasticsearchUtil.destinationClient(),
                        bulkRequest);
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}

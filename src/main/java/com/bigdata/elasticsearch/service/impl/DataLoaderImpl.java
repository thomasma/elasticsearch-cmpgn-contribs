package com.bigdata.elasticsearch.service.impl;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.io.File;
import java.util.Map;
import java.util.UUID;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.aver.fft.RecordListener;
import org.aver.fft.Transformer;
import org.aver.fft.TransformerFactory;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import com.bigdata.elasticsearch.domain.Contribution;
import com.bigdata.elasticsearch.service.DataLoader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class DataLoaderImpl implements DataLoader, InitializingBean,
        Destroyable {
    private Client client;

    private ObjectMapper mapper = new ObjectMapper();

    /***/
    public void loadData(File dataFile) {
        Transformer spec = TransformerFactory
                .getTransformer(Contribution.class);
        spec.parseFlatFile(dataFile, new RecordListener() {
            public boolean foundRecord(Object o) {
                final Contribution contrib = (Contribution) o;
                String json = null;
                try {
                    json = mapper.writeValueAsString(contrib);
                } catch (JsonProcessingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                IndexResponse response = client
                        .prepareIndex("contributions", "year2012",
                                UUID.randomUUID().toString()).setSource(json)
                        .execute().actionGet();
                return true;
            }

            public boolean unresolvableRecord(String rec) {
                // nothing in here for now
                return true;
            }
        });
    }

    @Override
    public long getTotalCount() {
        CountResponse response = client.prepareCount("contributions")
                .setQuery(termQuery("_type", "year2012")).execute().actionGet();
        return response.getCount();
    }

    /***/
    public void getContributionsByCandName(String candName, Double amtEqGtThan) {
        QueryBuilder matchQuery = QueryBuilders.matchQuery("candNm", candName).operator(Operator.AND);
        FilterBuilder contribRangeFilter = FilterBuilders.rangeFilter(
                "contbReceiptAmt").gte(amtEqGtThan);

        StatsBuilder statsBuilder = AggregationBuilders.stats("stat1").field(
                "contbReceiptAmt");
        SearchRequestBuilder request = client
                .prepareSearch("contributions")
                .addSort(
                        SortBuilders.fieldSort("contbReceiptAmt").order(
                                SortOrder.DESC))
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(matchQuery)
                .setPostFilter(contribRangeFilter)
                .addAggregation(statsBuilder)
                .setFrom(0)
                .setSize(100)
                .addFields("contbrNm", "candNm", "contbrEmployer",
                        "contbReceiptAmt");
        System.out.println("SEARCH QUERY: " + request.toString());

        SearchResponse response = request.execute().actionGet();
        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            Map<String, SearchHitField> fields = hit.getFields();
            System.out.println(hit.getId() + ", contbrEmployer="
                    + fields.get("contbrEmployer").getValue().toString());
        }
    }

    public void afterPropertiesSet() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch").build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(
                        "localhost", 9300));
    }

    public void destroy() throws DestroyFailedException {
        if (client != null) {
            client.close();
        }
    }

    public boolean isDestroyed() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * ONLY FOR TESTING TO PRINT QUERY.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        DataLoaderImpl dl = new DataLoaderImpl();
        dl.afterPropertiesSet();
        dl.getContributionsByCandName("Marco Rubio", 2000d);
        dl.destroy();
    }
}

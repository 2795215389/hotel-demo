package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * author:JiangSong
 * Date:2023/6/20
 **/

@SpringBootTest
public class HotelSearchTest {
    @Autowired
    private IHotelService hotelService;
    private RestHighLevelClient client;
    @BeforeEach
    void setUp(){
        this.client=new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.87.100:9200")
        ));
    }
    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

    @Test
    void testMatchAll() throws IOException {
          //1准备Request对象
          SearchRequest request=new SearchRequest("hotel");
          //2准备DSL
          request.source().query(QueryBuilders.matchAllQuery());
          //3发送请求
          SearchResponse response=client.search(request, RequestOptions.DEFAULT);
          //解析结果
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits().value;
        System.out.println("共搜索到："+ total + "条数据");
        for(SearchHit hit : hits){
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json,HotelDoc.class);
            System.out.println("hotelDoc = "+ hotelDoc);
        }
        //System.out.println(response);
      }

      @Test
     void testMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        parseResponse(response);
     }


     private void parseResponse(SearchResponse response){
         SearchHits searchHits = response.getHits();
         long total = searchHits.getTotalHits().value;
         System.out.println("共搜索到："+ total + "条数据");
         for(SearchHit hit : searchHits){
             String json = hit.getSourceAsString();
             HotelDoc hotelDoc = JSON.parseObject(json,HotelDoc.class);
             System.out.println("hotelDoc = "+ hotelDoc);
         }
     }

     private void parseHighLight(SearchResponse response){
         SearchHits searchHits = response.getHits();
         long total = searchHits.getTotalHits().value;
         System.out.println("共搜索到："+ total + "条数据");
         for(SearchHit hit : searchHits){
             String json = hit.getSourceAsString();
             HotelDoc hotelDoc = JSON.parseObject(json,HotelDoc.class);
             Map<String, HighlightField> highlightFields = hit.getHighlightFields();
             if(!CollectionUtils.isEmpty(highlightFields)){
                 HighlightField highlightField = highlightFields.get("name");
                 String name = highlightField.getFragments()[0].string();
                 hotelDoc.setName(name);
             }
             System.out.println("hotelDoc = "+ hotelDoc);
         }
     }

    @Test
     void testBoolQuery() throws IOException{
        SearchRequest request = new SearchRequest("hotel");
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("city","上海"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source().query(boolQuery);
        parseResponse(client.search(request,RequestOptions.DEFAULT));
     }

    @Test
    void testPageAndSort() throws IOException{
        int page = 2, size =5;
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        request.source().from((page - 1) * size).size(size);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        parseResponse(response);
    }

    @Test
    void testHighlight() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        // 2.1.query
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        // 2.2.高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        parseHighLight(response);

    }

    @Test
    void testAggregation() throws IOException {
        //1.准备请求对象
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(10));
        //3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            System.out.println(key);
        }
    }


    //自动补全
    @Test
    void testSuggest() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("bj")
                        .skipDuplicates(true)
                        .size(10)
        ));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
        for (CompletionSuggestion.Entry.Option option : options) {
            String text = option.getText().toString();
            System.out.println(text);
        }


        System.out.println(response);
    }
}

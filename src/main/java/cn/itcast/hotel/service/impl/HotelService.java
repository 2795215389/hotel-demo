package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    private RestHighLevelClient client;
    //非业务代码可以抛出异常，业务代码用try-catch显示捕获
    @Override
    public PageResult search(RequestParams params) {
        SearchRequest request = new SearchRequest("hotel");
        //条件过滤
        buildBasicQuery(params, request);
        //排序
        String location = params.getLocation();
        if(!StringUtils.isEmpty(location)){
            request.source().sort(SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                    .order(SortOrder.ASC)
                    .unit(DistanceUnit.KILOMETERS));
        }
        //分页，参与运算的先拆箱
        int page = params.getPage();
        int size = params.getSize();
        request.source().from((page -1) * size).size(size);
        try{
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return  parseResponse(response);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        try {
            //1.准备请求对象
            SearchRequest request = new SearchRequest("hotel");
            //2.准备DSL
            buildAggregation(request);
            buildBasicQuery(params, request);
            //3.发出请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.解析结果
            Map<String, List<String>> result = new HashMap<>();
            List<String> brandList = getAggByName(response, "brandAgg");
            result.put("brand", brandList);
            List<String> cityList = getAggByName(response, "cityAgg");
            result.put("city", cityList);
            List<String> starList = getAggByName(response, "starAgg");
            result.put("starName", starList);
            return result;
        }catch (IOException e){
            throw new RuntimeException();
        }

    }

    private static List<String> getAggByName(SearchResponse response, String aggName) {
        List<String> brandList = new ArrayList<>();
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            brandList.add(key);
        }
        return brandList;
    }

    private static void buildAggregation(SearchRequest request) {
        request.source().aggregation(
                AggregationBuilders.terms("brandAgg").field("brand").size(100));
        request.source().aggregation(
                AggregationBuilders.terms("cityAgg").field("city").size(100));
        request.source().aggregation(
                AggregationBuilders.terms("starAgg").field("starName").size(100));
    }

    //ctrl + alt + m   抽取代码
    private static void buildBasicQuery(RequestParams params, SearchRequest request) {
        // 1.准备Boolean查询
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // 1.1.关键字搜索，match查询，放到must中
        String key = params.getKey();
        if (org.apache.commons.lang3.StringUtils.isNotBlank(key)) {
            // 不为空，根据关键字查询
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        } else {
            // 为空，查询所有
            boolQuery.must(QueryBuilders.matchAllQuery());
        }

        // 1.2.品牌
        String brand = params.getBrand();
        if (StringUtils.isNotBlank(brand)) {
            boolQuery.filter(QueryBuilders.termQuery("brand", brand));
        }
        // 1.3.城市
        String city = params.getCity();
        if (StringUtils.isNotBlank(city)) {
            boolQuery.filter(QueryBuilders.termQuery("city", city));
        }
        // 1.4.星级
        String starName = params.getStarName();
        if (StringUtils.isNotBlank(starName)) {
            boolQuery.filter(QueryBuilders.termQuery("starName", starName));
        }
        // 1.5.价格范围
        Integer minPrice = params.getMinPrice();
        Integer maxPrice = params.getMaxPrice();
        if (minPrice != null && maxPrice != null) {
            maxPrice = maxPrice == 0 ? Integer.MAX_VALUE : maxPrice;
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(minPrice).lte(maxPrice));
        }

        // 2.算分函数查询
        FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
                boolQuery, // 原始查询，boolQuery
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{ // function数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                QueryBuilders.termQuery("isAD", true), // 过滤条件
                                ScoreFunctionBuilders.weightFactorFunction(10) // 算分函数
                        )
                }
        );

        // 3.设置查询条件
        request.source().query(functionScoreQuery);
    }

    private PageResult parseResponse(SearchResponse response){
        SearchHits searchHits = response.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到："+ total + "条数据");
        for(SearchHit hit : searchHits){
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json,HotelDoc.class);
            //地理位置sort和source在同一级别，也就是hit下一级，解析hit集合
            Object[] sortValues = hit.getSortValues();
            if(sortValues != null && sortValues.length > 0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
        }
        return new PageResult(total,hotels);
    }


    //自动补全
    @Override
    public List<String> getSuggestions(String prefix) {
        try {
            // 1.准备Request
            SearchRequest request = new SearchRequest("hotel");
            // 2.准备DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(prefix)
                            .skipDuplicates(true)
                            .size(10)
            ));
            // 3.发起请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4.解析结果
            Suggest suggest = response.getSuggest();
            // 4.1.根据补全查询名称，获取补全结果
            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
            // 4.2.获取options
            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
            // 4.3.遍历
            List<String> list = new ArrayList<>(options.size());
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                list.add(text);
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            // 1.准备Request
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            // 2.发送请求
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            // 0.根据id查询酒店数据
            Hotel hotel = getById(id);
            // 转换为文档类型
            HotelDoc hotelDoc = new HotelDoc(hotel);

            // 1.准备Request对象
            IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
            // 2.准备Json文档
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            // 3.发送请求
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

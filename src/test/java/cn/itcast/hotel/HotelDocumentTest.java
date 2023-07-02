package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

/**
 * author:JiangSong
 * Date:2023/6/20
 **/

@SpringBootTest
public class HotelDocumentTest {
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
    void testAddDocument() throws IOException {
        Hotel hotel=hotelService.getById(61083L);
        HotelDoc hotelDoc=new HotelDoc(hotel);
        //1请求对象
        IndexRequest request=new IndexRequest("hotel").id(hotel.getId().toString());
        //2准备Json文件
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        //3发送请求
        client.index(request, RequestOptions.DEFAULT);
    }
    
    @Test
    void testGetDocumentById() throws IOException {
        //Get请求
        GetRequest request = new GetRequest("hotel","61083");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //解析结果
        String json=response.getSourceAsString();
        HotelDoc hotelDoc=JSON.parseObject(json,HotelDoc.class);
        System.out.println(hotelDoc);
    }
    //局部更新
    @Test
    void testUpdateDocumentById()throws Exception{
        UpdateRequest request=new UpdateRequest("hotel","61083");
        //准备参数
        request.doc(
                "price","666",
                "starName","四钻"
        );
        client.update(request,RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocument()throws IOException{
        DeleteRequest request=new DeleteRequest("hotel","61083");
        client.delete(request,RequestOptions.DEFAULT);
    }
    //批量导入
    @Test
    void testBulkRequest()throws IOException{
        List<Hotel> hotels = hotelService.list();
        //hotels.stream().map(h-> new HotelDoc(h)).collect(Collectors.toList());

        //批量请求对象
        BulkRequest request=new BulkRequest();
        //准备参数，添加多个请求
        for(Hotel hotel:hotels){
            HotelDoc hotelDoc=new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
//        request.add(new IndexRequest("hotel").id("61038").source("json",XContentType.JSON));
//        request.add(new IndexRequest("hotel").id("61039").source("json",XContentType.JSON));
        //发送请求
        client.bulk(request,RequestOptions.DEFAULT);
    }
}

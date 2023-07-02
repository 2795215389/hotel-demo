package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
class HotelDemoApplicationTests {
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
    void testInit(){
        System.out.println(client);
    }

    @Test
    void createHotelIndex() throws IOException {
        //1创建请求对象
        CreateIndexRequest request=new CreateIndexRequest("hotel");
        //2准备请求的参数
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        //3发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void deleteHotelIndex()throws IOException{
        DeleteIndexRequest request=new DeleteIndexRequest("hotel");
        client.indices().delete(request,RequestOptions.DEFAULT);
    }
    @Test
    void existsHotelIndex()throws IOException{
        GetIndexRequest request=new GetIndexRequest("hotel");
        boolean exists=client.indices().exists(request,RequestOptions.DEFAULT);
        System.out.println("索引库是否存在："+exists);
    }


}

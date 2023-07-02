package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * author:JiangSong
 * Date:2023/6/26
 **/

@SpringBootTest
public class HotelAggTest {
    @Autowired
    private IHotelService hotelService;

    @Test
    void contextLoad(){
    }
}

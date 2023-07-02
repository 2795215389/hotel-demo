package cn.itcast.hotel.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * author:JiangSong
 * Date:2023/6/25
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageResult {
    private Long total;
    private List<HotelDoc>hotels;
}

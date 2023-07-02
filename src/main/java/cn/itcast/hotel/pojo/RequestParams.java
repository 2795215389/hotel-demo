package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * author:JiangSong
 * Date:2023/6/25
 **/

@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;

    private String city;
    private String brand;
    private String starName;
    private Integer minPrice;
    private Integer maxPrice;
    private String location;
}

package free.man.pojo;

import lombok.Builder;
import lombok.Data;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/17 23:37
 **/
@Data
@Builder
public class Company {

    private String name;

    private String address;

}

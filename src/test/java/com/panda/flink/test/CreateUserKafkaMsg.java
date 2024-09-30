package com.panda.flink.test;

import com.google.gson.Gson;
import com.panda.flink.business.user.User;
import com.panda.flink.utils.KafkaUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Description 向kafka发送测试模拟用户数据
 * 如果在实际项目中使用的时候，kafka的生产者数据均在其他业务微服中，flink-service只处理基于flink流计算的逻辑。
 */
public class CreateUserKafkaMsg {
    static String upperNames = "赵,钱,孙,李,周,吴,郑,王,冯,陈,褚,卫,蒋,沈,韩,杨,朱,秦,尤,许,何,吕,施,张,孔,曹,严,华,金," + "魏,陶,姜,戚,谢,邹,喻,柏,水,窦,章,云,苏,潘,葛,奚,范,彭,郎,鲁,韦,昌,马,苗,凤,花,方,俞,任,袁,柳,酆,鲍,史,唐," + "费,廉,岑,薛,雷,贺,倪,汤,滕,殷,罗,毕,郝,邬,安,常,乐,于,时,傅,皮,卞,齐,康,伍,余,元,卜,顾,孟,平,黄,和,穆,萧," + "尹,姚,邵,湛,汪,祁,毛,禹,狄,米,贝,明,臧,计,伏,成,戴,谈,宋,茅,庞,熊,纪,舒,屈,项,祝,董,梁,杜,阮,蓝,闵,席,季," + "麻,强,贾,路,娄,危,江,童,颜,郭,梅,盛,林,刁,钟,徐,邱,骆,高,夏,蔡,田,樊,胡,凌,霍,虞,万,支,柯,昝,管,卢,莫,经," + "房,裘,缪,干,解,应,宗,丁,宣,贲,邓,郁,单,杭,洪,包,诸,左,石,崔,吉,钮,龚,程,嵇,邢,滑,裴,陆,荣,翁,荀,羊,於,惠," + "甄,曲,家,封,芮,羿,储,靳,汲,邴,糜,松,井,段,富,巫,乌,焦,巴,弓,牧,隗,山,谷,车,侯,宓,蓬,全,郗,班,仰,秋,仲,伊," + "宫,宁,仇,栾,暴,甘,钭,厉,戎,祖,武,符,刘,景,詹,束,龙,叶,幸,司,韶,郜,黎,蓟,薄,印,宿,白,怀,蒲,邰,从,鄂,索,咸," + "籍,赖,卓,蔺,屠,蒙,池,乔,阴,胥,能,苍,双,闻,莘,党,翟,谭,贡,劳,逄,姬,申,扶,堵,冉,宰,郦,雍,郤,璩,桑,桂,濮,牛," + "寿,通,边,扈,燕,冀,郏,浦,尚,农,温,别,庄,晏,柴,瞿,阎,充,慕,连,茹,习,宦,艾,鱼,容,向,古,易,慎,戈,廖,庾,终,暨," + "居,衡,步,都,耿,满,弘,匡,国,文,寇,广,禄,阙,东,欧,殳,沃,利,蔚,越,夔,隆,师,巩,厍,聂,晁,勾,敖,融,冷,訾,辛,阚," + "那,简,饶,空,曾,毋,沙,乜,养,鞠,须,丰,巢,关,蒯,相,查,後,荆,红,游,竺,权,逯,盖,益,桓,公";
    static String upperNums = "壹,贰,叁,肆,伍,陆,柒,捌,玖,拾,佰,仟,万,亿,元,角,分,零";
    static String[] genders = new String[]{"男", "女"};
    static String[] addresss = new String[]{"河北省", "山西省", "辽宁省", "吉林省", "黑龙江省", "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省", "河南省", "湖北省", "湖南省", "广东省", "海南省", "四川省", "贵州省", "云南省", "陕西省", "甘肃省", "青海省", "台湾省", "北京市", "天津市", "上海市", "重庆市", "广西壮族自治区", "内蒙古自治区", "西藏自治区", "宁夏回族自治区", "新疆维吾尔自治区"};

    /**
     * 用户信息：
     * 用户ID、用户名称、用户性别、地址信息、联系方式、年龄、注册时间
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String[] userNames = StringUtils.split(upperNames, ",");
        String[] nums = StringUtils.split(upperNums, ",");
        Map<String, String> genderMap = new HashMap<>();
        Map<String, String> addressMap = new HashMap<>();

        //生产者发送消息
        KafkaUtils.KafkaStreamServer kafkaStreamServer = KafkaUtils.bulidServer().createKafkaStreamServer("127.0.0.1", 9092);
        String topic = "flink-add-user";

        //模拟不停点创建模拟注册用户
        int i = 0;
        while (true) {
            String userId = DateFormatUtils.format(System.currentTimeMillis(), "yyyyMMddHHmmssSSS") + RandomUtils.nextInt(500, 9900);
            String userName = userNames[RandomUtils.nextInt(0, userNames.length)] + nums[RandomUtils.nextInt(0, nums.length)];
            String gender = genderMap.get(userName);
            if (gender == null) {
                gender = genders[i % 2];
                genderMap.put(userName, gender);
            }
            Integer age = new Random().nextInt(60) + 15;
            String address = addressMap.get(userName);
            if (address == null) {
                address = addresss[RandomUtils.nextInt(0, addresss.length)];
                addressMap.put(userName, address);
            }

            //订单生成时间
            Long registerTimeSeries = System.currentTimeMillis();
            String registerTime = DateFormatUtils.format(registerTimeSeries, "yyyy-MM-dd HH:mm:ss");
            Integer registerLabel = 1;
            String registerSource = "1";
            String phone = String.format("13%s%09d", (i + 1) % 9, i);
            //用户ID、用户名称、用户性别、地址信息、联系方式、年龄、注册时间
            User user = new User(userId, userName, gender, address, Long.parseLong(phone), String.valueOf(age), registerTime, registerLabel, registerSource, registerTimeSeries);
            String orderJson = new Gson().toJson(user);
            //System.out.println(orderJson);
            i++;
            //向kafka队列发送数据
            kafkaStreamServer.sendMsg(topic, orderJson);
            //模拟不同时间段的消费量
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(registerTimeSeries);
            int h = calendar.get(Calendar.HOUR_OF_DAY);
            int startInt = 700;
            if (8 > h) {
                startInt = 1500;
            } else if (h >= 8 && h < 18) {
                startInt = 300;
            } else if (h >= 18 && h < 22) {
                startInt = 100;
            }
            //线程休眠
            TimeUnit.MILLISECONDS.sleep(RandomUtils.nextInt(startInt, 3000));
        }
    }
}

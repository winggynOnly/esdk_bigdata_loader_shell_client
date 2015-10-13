package com.huawei.test;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import scala.Tuple2;

public class SqlSpark
{
    public static void main(String[] args)
        throws Exception
    {
    	//创建一个配置类SparkConf，然后创建一个SparkContext
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaSQLContext sqlCtx = new JavaSQLContext(ctx);
        
        //读取原文件数据,每一行记录转成RDD里面的一个元素
        JavaRDD<String> data = ctx.textFile(args[0]);
        
        //读取原文件数据,每一行记录转成RDD里面的一个元素
        JavaRDD<CarStreetTime> carStreetTime = data.map(new Function<String, CarStreetTime>()
        {
            private static final long serialVersionUID = 7019722250148213436L;
            
            public CarStreetTime call(String line)
                throws Exception
            {
            	//按逗号分割一行数据
                String[] parts = line.split(",");
                
                //将分割后的三个元素组成一个CarStreetTime
            	CarStreetTime carStreetTime = new CarStreetTime();
                carStreetTime.setStreetId(parts[0]);
                carStreetTime.setCarId(parts[1]);
                carStreetTime.setTime(Long.parseLong(parts[2]));
                return carStreetTime;
            }
        });
        
        // 转化为 schemaRDD数据集并注册一个数据表 
        JavaSchemaRDD schemaCarStreetTime = sqlCtx.applySchema(carStreetTime, CarStreetTime.class);
        schemaCarStreetTime.registerTempTable("CarStreetTime");
        
        // 在注册的数据表上执行
        //指定地点为{luxiang, shengze, dongxin, tongli, loufeng, shangtang, taicang, huqiu, wujiang, kunshan}
		//时间（单位：毫秒）段为{1421600000000~1431697533000}
        JavaSchemaRDD teenagers =
        	sqlCtx.sql(" SELECT count(*) as num, carId FROM CarStreetTime WHERE time >= 1421600000000 AND time <= 1431697533000 AND streetId IN ('luxiang','shengze','dongxin','tongli','loufeng','shangtang', 'taicang','huqiu','wujiang','kunshan') group by carId order by num desc ");
        
        // 查询结果为SchemaRDDs数据集，支持正规的 RDD操作.
        // 将查询数据集转化为两列数据，第一列为车牌号，第二列为车辆出现次数
        JavaRDD<Tuple2<String, Long>> map = teenagers.map(new Function<Row, Tuple2<String, Long>>()
        {
            private static final long serialVersionUID = -3722488141180116432L;
            
            public Tuple2<String, Long> call(Row row)
            {
            	//获取车辆出现的次数
                long num = row.getLong(0);
                //获取车牌号
                String carId = row.getString(1);
                Tuple2<String, Long> result = new Tuple2<String, Long>(carId, num);
                
                return result;
            }
        });
        
        //筛选出出现的次数最多前5辆信息并打印数据
        for (Tuple2<String, Long> d : map.take(5))
        {
            System.out.println("{" + d._1() + "," + d._2() + "}");
        }
    }
    
	/**
	 * 
	 * 车辆信息内部类
	 */
    public static class CarStreetTime implements Serializable
    {
        private static final long serialVersionUID = 3429656478937100909L;
        
        /**
         * 车牌号
         */
        private String carId;
        
        /**
         * 地址
         */
        private String streetId;
        
        /**
         * 时间
         */
        private long time;
        
        public String getCarId()
        {
            return carId;
        }
        
        public void setCarId(String carId)
        {
            this.carId = carId;
        }
        
        public String getStreetId()
        {
            return streetId;
        }
        
        public void setStreetId(String streetId)
        {
            this.streetId = streetId;
        }
        
        public long getTime()
        {
            return time;
        }
        
        public void setTime(long time)
        {
            this.time = time;
        }
        
    }

}

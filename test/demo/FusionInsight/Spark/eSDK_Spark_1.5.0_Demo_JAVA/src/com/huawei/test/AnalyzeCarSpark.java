package com.huawei.test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;


public class AnalyzeCarSpark implements Serializable
{
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) 
		throws Exception
	{
		//创建一个配置类SparkConf，然后创建一个SparkContext
		SparkConf conf = new SparkConf().setAppName("AnalyzeCarSpark");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		//读取原文件数据,每一行记录转成RDD里面的一个元素
		JavaRDD<String> data = jsc.textFile(args[0]);
		
		JavaRDD<Tuple3<String, String, Long>> carInfo = 
			data.map(new Function<String, Tuple3<String, String, Long>>()
		{
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple3<String, String, Long> call(String arg0)
						throws Exception {
					//按逗号分割一行数据
					String[] tokens = arg0.split(",");
					
					//将分割后的三个元素组成一个三元Tuple
					Tuple3<String, String, Long> carInfo = 
						new Tuple3<String, String, Long>(tokens[0], tokens[1], Long.parseLong(tokens[2]));
					return carInfo;
				}
				
				
		});
		
		//使用filter函数筛选出在多个不同地点指定时间段同时出现车辆信息
		final JavaRDD<Tuple3<String, String, Long>> specifyCar = 
			carInfo.filter(new Function<Tuple3<String, String, Long>, Boolean>()
		{

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple3<String, String, Long> arr)
					throws Exception {
				//指定地点为{luxiang, shengze, dongxin, tongli, loufeng, shangtang, taicang, huqiu, wujiang, kunshan}
				//时间（单位：毫秒）段为{1421600000000~1431697533000}
			    //arr._1()指地点，arr._3指时间
				Boolean isSpecify = (arr._1().equals("luxiang") || arr._1().equals("shengze") ||
					arr._1().equals("dongxin")	|| arr._1().equals("tongli") || arr._1().equals("loufeng")
					|| arr._1().equals("shangtang") || arr._1().equals("taicang") || arr._1().equals("huqiu")
					|| arr._1().equals("wujiang") || arr._1().equals("kunshan")) && arr._3() >= 1421600000000L && arr._3() <= 1431697533000L;
				return isSpecify;
			}

		});
		
		//汇总车辆在指定的时间指定的地点内出现的次数
		final Map<String, Object> countCar =
			specifyCar.mapToPair(new PairFunction<Tuple3<String, String, Long>, String, String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple3<String, String, Long> arg)
					throws Exception {
				//将车牌号作为key,以便countByKey接口按照车牌号来统计车辆出现的次数。
				Tuple2<String, String> NumAndStreat = new Tuple2<String, String>(arg._2(), arg._2());
				return NumAndStreat;
			}
		}).countByKey();
		
		//将Map对象序列化
		final Map<String, Object> countSerialize = new HashMap<String, Object>();
		countSerialize.putAll(countCar);
		
		//按车辆出现的次数降序排序。
		JavaPairRDD<Long, String> sortCar =
			specifyCar.mapToPair(new PairFunction<Tuple3<String, String, Long>, String, String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple3<String, String, Long> arg)
					throws Exception {
				
				//将车牌号作为key,以便groupByKey接口按照车牌号来对车辆出现的情况进行分组。
				Tuple2<String, String> NumAndStreat = new Tuple2<String, String>(arg._2(), arg._2());
				return NumAndStreat;
			}
		}).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>()
		{
			
			private static final long serialVersionUID = 1L;
			
			@Override
			public Tuple2<Long, String> call(
					Tuple2<String, Iterable<String>> arg0) throws Exception {
				//获取车牌号出现的次数
				Long count = (Long)countSerialize.get(arg0._1());
				//将车辆出现的次数作为key,以便 sortByKey接口按照车辆出现的次数进行降序排序
				Tuple2<Long, String> keyAndValue = new Tuple2<Long, String>(count, arg0._1());
				return keyAndValue;
			}
		}).sortByKey(false);
		
		//筛选出出现的次数最多前5辆信息并打印数据
		for (Tuple2<Long, String> d : sortCar.take(5))
		{
			System.out.println("{" + d._2() + "," + d._1() + "}");
		}
		
		//保存数据，path为"/tmp/input/duhuadong"加一个时间戳
		sortCar.saveAsTextFile("/tmp/input/duhuadong" + System.currentTimeMillis());
	}
}

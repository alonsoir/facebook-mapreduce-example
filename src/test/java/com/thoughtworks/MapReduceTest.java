package com.thoughtworks;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MapReduceTest {
	
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable,Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp(){
	
		FacebookMapper mapper = new FacebookMapper();
		FacebookBuzzCount.IntSumReducer reducer = new FacebookBuzzCount.IntSumReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper,reducer);
		
	}
	
	@Test
	public void testMapper() throws Exception {

//		Text val = new Text("bc;http://graph.facebook.com/BostonCollegeAthletics clemson;http://graph.facebook.com/107331052623060;");
		//ojito, esto se trata de sacar los likes que tiene el no se que identificado por 107331052623060, a dia y hora de hoy tiene 150708, lo logico es que cambie
		Text val = new Text("clemson;http://graph.facebook.com/107331052623060;");
		mapDriver.withInput(new LongWritable(), val);
		mapDriver.withOutput(new Text("clemson"), new IntWritable(150708));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws Exception {
		
		List<IntWritable> milista = new ArrayList<IntWritable>();
		milista.add(new IntWritable(150708));
//		milista.add(new IntWritable(10));
//		milista.add(new IntWritable(1));
//		milista.add(new IntWritable(100));
		//la operacion de reduccion consiste en consiste en aquel que tiene el mayor numero de dominios registrados, es decir el mayor de todos
		reduceDriver.withInput(new Text("clemson"), milista);
		reduceDriver.withOutput(new Text("clemson"), new IntWritable(150708));
		reduceDriver.runTest();
	}

}

package pl.consileon.exercise.java8;

import static org.junit.Assert.*;

import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Test;

public class WeatherStatsAnalyzerTest {

	private static final int HOURS_LEN= 24;

	protected Stream<WindDataRow> createInputStream() {
		Stream<WindDataRow> objStream = Stream.builder()
				.add(
					new WindDataRow(
						LocalDate.of(2014, Month.JANUARY, 10), 
						IntStream.iterate(0,  n -> n+1).limit(HOURS_LEN).toArray(),
						Stream.iterate(0.0,  n -> n+0.1).limit(HOURS_LEN).mapToDouble(n->n).toArray() 
					)
				).add(
					new WindDataRow(
						LocalDate.of(2014, Month.APRIL, 30), 
						IntStream.iterate(0,  n -> n+1).limit(HOURS_LEN).toArray(),
						Stream.iterate(0.0,  n -> n+0.05).limit(HOURS_LEN).mapToDouble(n->n).toArray() 
					)
				)
				.add( 
					new WindDataRow(
						LocalDate.of(2014, Month.DECEMBER, 4), 
						IntStream.iterate(10,  n -> n+1).limit(HOURS_LEN).toArray(),
						Stream.iterate(0.0,  n -> n+0.1).limit(HOURS_LEN).mapToDouble(n->n).toArray() 
					)
			).build().map(o-> (WindDataRow)o).peek(System.out::println);

		return objStream;
	}
	
	
	@Test
	public void testProcessData() {
		
		Stream<WindDataRow> objStream = this.createInputStream();
		List<LocalDate> processData = WeatherStatsAnalyzer.processData( x -> true,  new TimeSpanHours(0, 23), 0, 100, objStream);
		
		assertArrayEquals(
				new LocalDate[]{
						LocalDate.of(2014, Month.JANUARY, 1),
						LocalDate.of(2014, Month.APRIL, 1),
						LocalDate.of(2014, Month.DECEMBER, 1)
					}, 
				processData.toArray(new LocalDate[processData.size()] )  
			);
	}

	@Test
	public void testProcessData_empty() {
		
		Stream<WindDataRow> objStream = this.createInputStream();
		List<LocalDate> processData = WeatherStatsAnalyzer.processData( x -> false,  new TimeSpanHours(0, 23), 0, 100, objStream);
		
		assertEquals(0, processData.size());
	}
	
	
	@Test
	public void testProcessData_highWindLimit() {
		
		Stream<WindDataRow> objStream = this.createInputStream();
		List<LocalDate> processData = WeatherStatsAnalyzer.processData( x -> true,  new TimeSpanHours(0, 23), 20, 100, objStream);
		
		assertArrayEquals(
				new LocalDate[]{
						LocalDate.of(2014, Month.DECEMBER, 1)
					}, 
				processData.toArray(new LocalDate[processData.size()] )  
			);
	}
	
	@Test
	public void testProcessData_lowPercipLimit() {
		
		Stream<WindDataRow> objStream = this.createInputStream();
		List<LocalDate> processData = WeatherStatsAnalyzer.processData( x -> true,  new TimeSpanHours(0, 23), 0, 0.9, objStream);
		
		assertArrayEquals(
				new LocalDate[]{
						LocalDate.of(2014, Month.APRIL, 1)
					}, 
				processData.toArray(new LocalDate[processData.size()] )  
			);
	}
	
	
	@Test
	public void testProcessData_hourRange1() {
		
		Stream<WindDataRow> objStream = this.createInputStream();
		List<LocalDate> processData = WeatherStatsAnalyzer.processData( x -> true,  new TimeSpanHours(0, 1), 0, 0.1, objStream);
		
		assertArrayEquals(
				new LocalDate[]{
						LocalDate.of(2014, Month.DECEMBER, 1)
					}, 
				processData.toArray(new LocalDate[processData.size()] )  
			);
	}
	
	@Test
	public void testProcessData_hourRange2() {
		
		Stream<WindDataRow> objStream = this.createInputStream();
		List<LocalDate> processData = WeatherStatsAnalyzer.processData( x -> true,  new TimeSpanHours(22, 23), 24, 100, objStream);
		
		assertArrayEquals(
				new LocalDate[]{
						LocalDate.of(2014, Month.DECEMBER, 1)
					}, 
				processData.toArray(new LocalDate[processData.size()] )  
			);
	}

}

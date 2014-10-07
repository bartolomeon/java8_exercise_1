package pl.consileon.exercise.java8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Time period in a day in between given start and end hour
 */
class TimeSpanHours {
	int startHour;
	int endHour;
	public TimeSpanHours(int startHour, int endHour) {
		super();
		if (startHour<0 || startHour>23 || endHour<0 || endHour>23 || startHour>endHour) {
			throw new IllegalArgumentException("both startDate/endDate must be in range 0..23 and startDate<endDate");
		}
		this.startHour = startHour;
		this.endHour = endHour;
	}
	public int getStartHour() {
		return startHour;
	}
	public int getEndHour() {
		return endHour;
	}
	public int getDiffHours() {
		return endHour - startHour;
	}
}



/**
 * Wrapper for a single row from CSV file.
 * - row 0 is a date
 * - rows 1-24 are wind measurements in knots (integers) for each hour between 00h ... 23h
 * - rows 25-48 are percipation measurements (floating point) for each hour between 00h ... 23h
 */
class WindDataRow {
	LocalDate date;
	int wind[];
	double percip[];
	public WindDataRow(LocalDate date, int[] wind, double[] percip) {
		super();
		this.date = date;
		this.wind = wind;
		this.percip = percip;
	}

	private static final DateTimeFormatter  dateFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");
	
	public static WindDataRow fromCSVLine(String[] columns) {
		LocalDate date = LocalDate.parse(columns[0], dateFormatter);

		int[] wind = Arrays.stream(columns).skip(1).limit(24).mapToInt(Integer::valueOf).toArray();
		double percip[] = Arrays.stream(columns).skip(25).mapToDouble(Double::valueOf).toArray();
		
		return new WindDataRow(date, wind, percip);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("WindDataRow [date=").append(date).append(", wind=")
				   .append(Arrays.toString(wind)).append(", percip=").append(Arrays.toString(percip))
				   .append("]");
		return builder.toString();
	}
}

public class WeatherStatsAnalyzer {
	
	public List<LocalDate> findTheGoodTimes(Path path, 
			Predicate<WindDataRow> filterPredicateFunc, 
			TimeSpanHours hours, double minWind, double minPerc) throws IOException, ParseException {
							
		Objects.requireNonNull(path);

		List<LocalDate> results;
		try (Stream<String> linesStream = Files.lines(path)) {

			Stream<WindDataRow> objStream = linesStream.skip(1)
					.map(line -> line.split("\t")).map(WindDataRow::fromCSVLine);
			
			results = processData(filterPredicateFunc, hours, minWind, minPerc, objStream);

		}

		return results;

	}

	

	/**
	 * @param filterPredicateFunc
	 * @param hours - constraints for the hours of day to be taken into consideration
	 * @param minWind - min wind speed in knots
	 * @param maxPerc - max required percipation (rain stats average)
	 * @param objStream  input data stream of WindDataRow POJOS
	 * @return
	 */
	private List<LocalDate> processData(Predicate<WindDataRow> filterPredicateFunc,
			TimeSpanHours hours, double minWind, double maxPerc,
			Stream<WindDataRow> objStream) {

		int hourLimit = hours.getDiffHours();

		List<LocalDate> results;
		Map<LocalDate, List<WindDataRow>> groupedByMonths =  objStream
				.filter(filterPredicateFunc)
				.filter(e -> Arrays.stream(e.percip).skip(hours.getStartHour()).limit(hourLimit)
								.average().getAsDouble() < maxPerc)
				.filter(e -> Arrays.stream(e.wind)
								.skip(hours.getStartHour()).limit(hourLimit)
								.average().getAsDouble() > minWind )
				.collect( 
						Collectors.groupingBy(
								e -> LocalDate.of(e.date.getYear(), e.date.getMonthValue(), 1)
						)
				);
		
		results = groupedByMonths.keySet().stream().sorted()
				.peek(System.out::println)
				.collect(Collectors.toList());
		return results;
	}


	public static void main(String[] args) throws IOException, ParseException {
		
		LocalDate startDate = LocalDate.of(2013, Month.DECEMBER, 31);
		LocalDate endDate = LocalDate.of(2014, Month.JULY, 1);
		
		final TimeSpanHours hours = new TimeSpanHours(9, 18);
		
		
		Predicate<WindDataRow> filterByTimePredicateFunc = 
				e  -> e.date.isAfter(startDate) && e.date.isBefore(endDate);
		
		new WeatherStatsAnalyzer().findTheGoodTimes(
				Paths.get("wg_data.csv"), filterByTimePredicateFunc, hours, 16, 0.2
		);
	}
	
}

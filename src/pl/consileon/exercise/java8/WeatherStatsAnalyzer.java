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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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
		double percip[] = Arrays.stream(columns).skip(24).mapToDouble(Double::valueOf).toArray();
		
		return new WindDataRow(date, wind, percip);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("WindDataRow [date=").append(date).append(", wind=").append(Arrays.toString(wind)).append(", percip=").append(Arrays.toString(percip))
				.append("]");
		return builder.toString();
	}
}

public class WeatherStatsAnalyzer {

	public static void main(String[] args) throws IOException, ParseException {
		final LocalDate startDate = LocalDate.of(2013, Month.DECEMBER, 31);
		final LocalDate endDate = LocalDate.of(2014, Month.JULY, 1);

		final int startHour = 9;
		final int endHour = 18;

		Function<Stream<WindDataRow>, Stream<WindDataRow>> filterByTimeFunc = 
				dataStream -> dataStream.filter(e -> (e.date.isAfter(startDate)) )
														   .filter(e -> e.date.isBefore(endDate)  );

		
		new WeatherStatsAnalyzer().findTheGoodTimes(
				Paths.get("wg_data.csv"), filterByTimeFunc, startHour, endHour, 16, 0.2
		);
		
	}

	
	
	public List<LocalDate> findTheGoodTimes(Path path, 
			Function<Stream<WindDataRow>, Stream<WindDataRow>> extraFilterFunc, 
			int startHour, int endHour, double minWind, double minPerc) throws IOException, ParseException {
							
		Objects.requireNonNull(path);

		int hourLimit = endHour - startHour;

		List<LocalDate> results;
		try (Stream<String> linesStream = Files.lines(path)) {

			Stream<WindDataRow> objStream = linesStream.skip(1)
					.map(line -> line.split("\t")).map(WindDataRow::fromCSVLine);
			
			Map<LocalDate, List<WindDataRow>> groupedByMonths = 
					extraFilterFunc.apply(objStream)
					.filter(e -> Arrays.stream(e.percip).skip(startHour).limit(hourLimit)
									.average().getAsDouble() < 0.2)
					.filter(e -> Arrays.stream(e.wind)
									.skip(startHour).limit(hourLimit)
									.average().getAsDouble() > 16 )
					.collect( 
							Collectors.groupingBy(
									e -> LocalDate.of(e.date.getYear(), e.date.getMonthValue(), 1)
							)
					);
			
			results = groupedByMonths.keySet().stream().sorted()
					.peek(System.out::println)
					.collect(Collectors.toList());

		}

		return results;

	}


}

/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * The "Popular Places" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes. <=> sliding window (size=15, slide=5)
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class PopularPlacesExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);
		final int popThreshold = params.getInt("threshold", 20);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// find n most popular spots
		DataStream<?> popularPlaces = rides
				// remove all rides which are not within NYC
				.filter(new NYCFilter())
				// match ride to grid cell and event type (start or end)
				.map(new GridCellMatcher())
                .keyBy(new KeySelector<Tuple3<Integer,Boolean, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<Integer, Boolean, Integer> value) throws Exception {
                        return value.f0.toString() + value.f1.toString();
                    }
                })
                .timeWindow(Time.minutes(15), Time.minutes(5))
//                .allowedLateness(Time.seconds(60))
                .sum(2)
                .process(new ProcessFunction<Tuple3<Integer,Boolean,Integer>, Tuple5<Float, Float, Long, Boolean, Integer>>() {
                    @Override
                    public void processElement(Tuple3<Integer, Boolean, Integer> value, Context ctx, Collector<Tuple5<Float, Float, Long, Boolean, Integer>> out) throws Exception {
                        out.collect(Tuple5.of(GeoUtils.getGridCellCenterLon(value.f0),
                                GeoUtils.getGridCellCenterLat(value.f0),
                                ctx.timestamp() + 1,  // window end
                                value.f1,
                                value.f2));
                    }
                })
                .filter(new FilterFunction<Tuple5<Float, Float, Long, Boolean, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Float, Float, Long, Boolean, Integer> value) throws Exception {
                        return value.f4 >= popThreshold;  // conventional use case '>='
                    }
                });

		printOrTest(popularPlaces);

		env.execute("Popular Places");
	}

	/**
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple3<Integer, Boolean, Integer>> {

		@Override
		public Tuple3<Integer, Boolean, Integer> map(TaxiRide taxiRide) throws Exception {
            if (taxiRide.isStart)   // departure
		        return Tuple3.of(GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat), true, 1);
            else
                return Tuple3.of(GeoUtils.mapToGridCell(taxiRide.endLon, taxiRide.endLat), false, 1);
		}
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}
}

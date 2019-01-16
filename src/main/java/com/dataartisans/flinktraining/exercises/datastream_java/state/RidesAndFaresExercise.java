/*
 * Copyright 2017 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_java.state;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * The "Stateful Enrichment" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 *  <br/>
 *  Sln. 1: connect + flatMap. advan: no need to consider timestamp and out-of-order events problem. disad.: memory leak and overhead management
 *  Sln. 2: join + allowLateness. advan. : easy API. disad.: can't conquer out-of-order events problem if the lateness is too high.
 */
public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", pathToRideData);
		final String faresFile = params.get("fares", pathToFareData);

		final int delay = 60;					// at most 60 seconds of delay
		final int servingSpeedFactor = 1800; 	// 30 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> ride.isStart)
				.keyBy("rideId");

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
				.keyBy("rideId");

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares)
				.flatMap(new EnrichmentFunction());

		printOrTest(enrichedRides);

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

	    private Map<Long, Tuple2<TaxiRide, TaxiFare>> joinStream;

		@Override
		public void open(Configuration config) throws Exception {
			joinStream = new HashMap<>();
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
		    Tuple2<TaxiRide, TaxiFare> rideFare = joinStream.get(ride.rideId);
		    if (rideFare == null) { // new ride ID
                rideFare = Tuple2.of(ride, null);
                joinStream.put(ride.rideId, rideFare);
		    } else if (rideFare.f0 == null) {  // match a ride
		        rideFare.setField(ride, 0);
		        out.collect(rideFare);
		        joinStream.remove(ride.rideId);
            } else {  // duplicated ride
		        assert false;
            }
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            Tuple2<TaxiRide, TaxiFare> rideFare = joinStream.get(fare.rideId);
            if (rideFare == null) { // new ride ID
                rideFare = Tuple2.of(null, fare);
                joinStream.put(fare.rideId, rideFare);
            } else if (rideFare.f1 == null) {  // match a fare
                rideFare.setField(fare, 1);
                out.collect(rideFare);
                joinStream.remove(fare.rideId);
            } else {  // duplicated ride
                assert false;
            }
		}
	}
}
/*
 * Copyright 2018 data Artisans GmbH
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

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.data-artisans.com).
 * <p>
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */
public class HourlyTipsExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {

        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToFareData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

        DataStream<Tuple2<Long, Double>> tipStream = fares.keyBy(new KeySelector<TaxiFare, Long>() {
            @Override
            public Long getKey(TaxiFare value) throws Exception {
                return value.driverId;
            }
        }).window(TumblingEventTimeWindows.of(Time.hours(1)))
                .allowedLateness(Time.seconds(maxEventDelay))
                .aggregate(new AggregateFunction<TaxiFare, Tuple2<Long, Double>, Tuple2<Long, Double>>() {

                    @Override
                    public Tuple2<Long, Double> createAccumulator() {
                        return Tuple2.of(null, 0.0);
                    }

                    @Override
                    public Tuple2<Long, Double> add(TaxiFare value, Tuple2<Long, Double> accumulator) {
                        accumulator.setFields(value.driverId, accumulator.f1 + value.tip);
                        return accumulator;
                    }

                    @Override
                    public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                }).filter(new FilterFunction<Tuple2<Long, Double>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Double> value) throws Exception {
                        return value.f0 != null;
                    }
                });

        // windowed results
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = tipStream.windowAll(TumblingEventTimeWindows.of(Time.hours(1))).process(new ProcessAllWindowFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Float>, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Tuple2<Long, Double>> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
                // Infinity value would cause serialization problem
                Tuple2<Long, Double> maxTip = Tuple2.of(null, Double.NEGATIVE_INFINITY);
                for (Tuple2<Long, Double> tip : elements) {
                    if (tip.f1 > maxTip.f1) {
                        maxTip = tip;
                    }
                }
                out.collect(Tuple3.of(context.window().getEnd(), maxTip.f0, maxTip.f1.floatValue()));
            }
        });

        printOrTest(hourlyMax);

        // execute the transformation pipeline
        env.execute("Hourly Tips (java)");
    }

}

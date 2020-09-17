/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
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
package io.github.streamingwithflink.chapter5.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Flink SourceFunction to generate random SmokeLevel events.
 */
public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    // flag indicating whether source is still running
    private boolean running = true;

    /**
     * Continuously emit one smoke level event per second.
     */
    @Override
    public void run(SourceContext<SmokeLevel> srcCtx) throws Exception {

        // initialize random number generator
        Random rand = new Random();

        // emit data until being canceled
        while (running) {

            if (rand.nextGaussian() > 0.8) {
                // emit a high SmokeLevel
                srcCtx.collect(SmokeLevel.HIGH);
            } else {
                srcCtx.collect(SmokeLevel.LOW);
            }

            // wait for 1 second
            Thread.sleep(1000);
        }
    }

    /**
     * Cancel the emission of smoke level events.
     */
    @Override
    public void cancel() {
        this.running = false;

    }
}

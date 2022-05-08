/*
 * (c) Copyright 2022 James Baker. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jbaker.loom.raft.simulation;

import java.lang.reflect.Field;
import java.util.concurrent.Executor;

/**
 * Applies hack which lets us use a custom scheduler in Project Loom. What hack is this?
 *
 * Project Loom supports a custom scheduler. It's a clear part of the project, but was removed from the API for the
 * first preview release <a href="https://github.com/openjdk/loom/commit/cad26ce74c98e28854f02106117fe03741f69ba0">here</a>.
 * The effect of this is that the virtual threads all run in the same executor.
 */
public final class HackVirtualThreads {
    private HackVirtualThreads() {}

    public static Thread.Builder.OfVirtual virtualThreadBuilderFor(Executor executor) {
        try {
            Thread.Builder.OfVirtual builder = Thread.ofVirtual();
            Class<?> clazz = Class.forName("java.lang.ThreadBuilders$VirtualThreadBuilder");
            Field scheduler = clazz.getDeclaredField("scheduler");
            scheduler.setAccessible(true);
            scheduler.set(builder, executor);
            return builder;
        } catch (NoSuchFieldException | ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}

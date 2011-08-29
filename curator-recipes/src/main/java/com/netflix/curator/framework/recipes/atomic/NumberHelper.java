/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.curator.framework.recipes.atomic;

public interface NumberHelper<T>
{
    public byte[] valueToBytes(T newValue);

    public T bytesToValue(byte[] data);

    public T one();

    public T negativeOne();

    public T negate(T value);

    public T add(T a, T b);

    public MutableAtomicValue<T> newMutableAtomicValue();
}

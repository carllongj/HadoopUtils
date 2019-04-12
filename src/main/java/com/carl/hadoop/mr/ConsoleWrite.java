/*
 * Copyright 2019 carllongj
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

package com.carl.hadoop.mr;

import com.carl.hadoop.config.HadoopParam;

import java.io.IOException;

/**
 * @author carllongj
 * 2019/4/11 20:32
 */
public class ConsoleWrite implements WriteStrategy {

    @Override
    public void write(Object o1, Object o2) {
        System.out.println(o1.toString() + this.getSplitValue() + o2.toString());
    }

    @Override
    public String getSplitValue() {
        return HadoopParam.KEY_VALUE_SPLIT_STANDARD;
    }

    @Override
    public void close() throws IOException {
        //nothing to do
    }
}
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

package com.carl.hadoop.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author carllongj
 * 2019/4/11 20:19
 */
public class IOUtils {
    /**
     * 创建字符输入缓冲流,不根据平台,默认使用UTF-8编码
     *
     * @param file 绑定的文件
     * @return 带缓冲的字符输入流
     */
    public static BufferedReader createReader(File file) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(file), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return reader;
    }

    /**
     * 创建字符输入缓冲流
     *
     * @param path
     * @return
     */
    public static BufferedReader createReader(String path) {
        return createReader(new File(path));
    }
}

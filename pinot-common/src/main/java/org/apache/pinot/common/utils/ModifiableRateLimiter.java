/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.common.utils;

import com.google.common.util.concurrent.RateLimiter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ModifiableRateLimiter {

  private int rate = 1;
  private boolean enabled = true;
  private RateLimiter rateLimiter = RateLimiter.create(rate);

  public ModifiableRateLimiter(String ratePath) {
    new Thread(() -> {
      while (true) {
        Properties prop = new Properties();
        try (InputStream rateFile = new FileInputStream(new File(ratePath))) {
          prop.load(rateFile);
          this.enabled = Boolean.parseBoolean(prop.getProperty("enabled"));
          int rate = Integer.parseInt(prop.getProperty("rate"));
          if (rate != this.rate) {
            this.rate = rate;
            rateLimiter = RateLimiter.create(rate);
          }
          Thread.sleep(3000);
        } catch (IOException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }).start();
  }

  public void acquire() {
    if (enabled) {
      rateLimiter.acquire();
    }
  }


//  public static void main(String[] args) {
//    ModifiableRateLimiter rateLimiter = new ModifiableRateLimiter();
//    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
//    while(true) {
//      rateLimiter.acquire();
//      System.out.println(dateFormat.format(new Date()));
//    }
//  }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.tools;

import picocli.CommandLine;

/**
 * Command-line access to Hive Metastore
 */
@CommandLine.Command(
        name = "MetastoreTool",
        mixinStandardHelpOptions = true, version = "1.0",
        subcommands = {CommandLine.HelpCommand.class},
        showDefaultValues = true)
public class MetastoreTool implements Runnable {
  @CommandLine.Option(names = {"-H", "--host"}, description = "HMS Host", paramLabel = "URI")
  private String host = "localhost";

  public static void main(String[] args) {
    CommandLine.run(new MetastoreTool(), args);
  }

  @Override
  public void run() {
    System.out.println("Hello, World " + host);
  }
}

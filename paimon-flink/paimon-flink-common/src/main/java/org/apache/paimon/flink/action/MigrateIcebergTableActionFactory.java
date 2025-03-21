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

package org.apache.paimon.flink.action;

import java.util.Map;
import java.util.Optional;

/** Action Factory for {@link MigrateIcebergTableAction}. */
public class MigrateIcebergTableActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "migrate_iceberg_table";

    private static final String OPTIONS = "options";
    private static final String PARALLELISM = "parallelism";

    private static final String ICEBERG_OPTIONS = "iceberg_options";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {

        String sourceTable = params.get(TABLE);
        Map<String, String> catalogConfig = catalogConfigMap(params);
        String tableConf = params.get(OPTIONS);
        Integer parallelism =
                params.get(PARALLELISM) == null ? null : Integer.parseInt(params.get(PARALLELISM));

        String icebergOptions = params.get(ICEBERG_OPTIONS);

        MigrateIcebergTableAction migrateIcebergTableAction =
                new MigrateIcebergTableAction(
                        sourceTable, catalogConfig, icebergOptions, tableConf, parallelism);
        return Optional.of(migrateIcebergTableAction);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"migrate_iceberg_table\" runs a migrating job from iceberg to paimon.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  migrate_iceberg_table \\\n"
                        + "--table <database.table_name> \\\n"
                        + "--iceberg_options <key>=<value>[,<key>=<value>,...] \\\n"
                        + "[--catalog_conf <key>=<value] \\\n"
                        + "[--options <key>=<value>,<key>=<value>,...]");
    }
}

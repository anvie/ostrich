package com.twitter.ostrich.stats;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;

import java.util.UUID;

/**
 * Author: robin
 * Date: 7/24/13
 * Time: 1:08 PM
 */
public class CassandraBackedStatsConstant {

    public static final String COLUMN_FAMILY_NAME = "TimeUUID1";
    public static final ColumnFamily<String, UUID> COLUMN_FAMILY = new ColumnFamily<String, UUID>(
            COLUMN_FAMILY_NAME, StringSerializer.get(), TimeUUIDSerializer.get());
}

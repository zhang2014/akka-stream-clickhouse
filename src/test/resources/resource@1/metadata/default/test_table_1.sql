ATTACH TABLE test_table_1
(
    eventDate Date,
    eventId UInt64,
    eventName String,
    count Int32
) ENGINE = MergeTree(eventDate, (eventId, eventName), 8192)

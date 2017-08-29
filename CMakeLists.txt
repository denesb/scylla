##
## CMakeLists.txt to build scylla
##
## Supported generators:
## * Ninja (recommended)
## * Make
##
## TODO:
## * --tests-debuginfo equivalent
## * More testing
## * Test that compilation works on all supported platforms:
##      - Ubuntu >= 14.04
##      - Debian >= 8
##      - Fedora >= 25
##      - CentOS/RHEL >= 7 (?)
##

cmake_minimum_required(VERSION 3.1)
project(scylla CXX)

#
# List of all sources.
# Please keep each section sorted (for humans).
# Headers are here for IDEs (and are not passed to the compiler).
#
set(
    SCYLLA_SOURCES
# Core
    atomic_cell.hh
    atomic_cell_hash.hh
    atomic_cell_or_collection.hh
    auth/auth.cc
    auth/auth.hh
    auth/authenticated_user.cc
    auth/authenticated_user.hh
    auth/authenticator.cc
    auth/authenticator.hh
    auth/authorizer.cc
    auth/authorizer.hh
    auth/data_resource.cc
    auth/data_resource.hh
    auth/default_authorizer.cc
    auth/default_authorizer.hh
    auth/password_authenticator.cc
    auth/password_authenticator.hh
    auth/permission.cc
    auth/permission.hh
    bytes.cc
    bytes.hh
    bytes_ostream.hh
    cache_streamed_mutation.hh
    caching_options.hh
    canonical_mutation.cc
    canonical_mutation.hh
    cartesian_product.hh
    cell_locking.hh
    checked-file-impl.hh
    clocks-impl.cc
    clocks-impl.hh
    clustering_bounds_comparator.hh
    clustering_key_filter.hh
    clustering_ranges_walker.hh
    combine.hh
    compaction_strategy.hh
    compatible_ring_position.hh
    compound.hh
    compound_compat.hh
    compress.hh
    converting_mutation_partition_applier.hh
    counters.cc
    counters.hh
    cpu_controller.hh
    cql3/abstract_marker.cc
    cql3/abstract_marker.hh
    cql3/assignment_testable.hh
    cql3/attributes.cc
    cql3/attributes.hh
    cql3/cf_name.cc
    cql3/cf_name.hh
    cql3/column_condition.cc
    cql3/column_condition.hh
    cql3/column_identifier.cc
    cql3/column_identifier.hh
    cql3/column_specification.cc
    cql3/column_specification.hh
    cql3/constants.cc
    cql3/constants.hh
    cql3/cql3_type.cc
    cql3/cql3_type.hh
    cql3/cql_statement.hh
    cql3/error_collector.hh
    cql3/error_listener.hh
    cql3/functions/abstract_function.hh
    cql3/functions/aggregate_fcts.hh
    cql3/functions/aggregate_function.hh
    cql3/functions/bytes_conversion_fcts.hh
    cql3/functions/function.hh
    cql3/functions/function_call.hh
    cql3/functions/function_name.hh
    cql3/functions/functions.cc
    cql3/functions/functions.hh
    cql3/functions/native_aggregate_function.hh
    cql3/functions/native_function.hh
    cql3/functions/native_scalar_function.hh
    cql3/functions/scalar_function.hh
    cql3/functions/time_uuid_fcts.hh
    cql3/functions/token_fct.hh
    cql3/functions/uuid_fcts.hh
    cql3/index_name.cc
    cql3/index_name.hh
    cql3/keyspace_element_name.cc
    cql3/keyspace_element_name.hh
    cql3/lists.cc
    cql3/lists.hh
    cql3/maps.cc
    cql3/maps.hh
    cql3/multi_column_relation.hh
    cql3/operation.cc
    cql3/operation.hh
    cql3/operation_impl.hh
    cql3/operator.cc
    cql3/operator.hh
    cql3/query_options.cc
    cql3/query_options.hh
    cql3/query_options_fwd.hh
    cql3/query_processor.cc
    cql3/query_processor.hh
    cql3/relation.cc
    cql3/relation.hh
    cql3/restrictions/abstract_restriction.hh
    cql3/restrictions/forwarding_primary_key_restrictions.hh
    cql3/restrictions/multi_column_restriction.hh
    cql3/restrictions/primary_key_restrictions.hh
    cql3/restrictions/restriction.hh
    cql3/restrictions/restrictions.hh
    cql3/restrictions/single_column_primary_key_restrictions.hh
    cql3/restrictions/single_column_restriction.hh
    cql3/restrictions/single_column_restrictions.hh
    cql3/restrictions/statement_restrictions.cc
    cql3/restrictions/statement_restrictions.hh
    cql3/restrictions/term_slice.hh
    cql3/restrictions/token_restriction.hh
    cql3/result_set.cc
    cql3/result_set.hh
    cql3/selection/abstract_function_selector.cc
    cql3/selection/abstract_function_selector.hh
    cql3/selection/aggregate_function_selector.hh
    cql3/selection/field_selector.hh
    cql3/selection/raw_selector.hh
    cql3/selection/scalar_function_selector.hh
    cql3/selection/selectable.cc
    cql3/selection/selectable.hh
    cql3/selection/selectable_with_field_selection.hh
    cql3/selection/selection.cc
    cql3/selection/selection.hh
    cql3/selection/selector.cc
    cql3/selection/selector.hh
    cql3/selection/selector_factories.cc
    cql3/selection/selector_factories.hh
    cql3/selection/simple_selector.cc
    cql3/selection/simple_selector.hh
    cql3/selection/writetime_or_ttl.hh
    cql3/selection/writetime_or_ttl_selector.hh
    cql3/sets.cc
    cql3/sets.hh
    cql3/single_column_relation.cc
    cql3/single_column_relation.hh
    cql3/statements/alter_keyspace_statement.cc
    cql3/statements/alter_keyspace_statement.hh
    cql3/statements/alter_table_statement.cc
    cql3/statements/alter_table_statement.hh
    cql3/statements/alter_type_statement.cc
    cql3/statements/alter_type_statement.hh
    cql3/statements/alter_user_statement.cc
    cql3/statements/alter_user_statement.hh
    cql3/statements/alter_view_statement.cc
    cql3/statements/alter_view_statement.hh
    cql3/statements/authentication_statement.cc
    cql3/statements/authentication_statement.hh
    cql3/statements/authorization_statement.cc
    cql3/statements/authorization_statement.hh
    cql3/statements/batch_statement.cc
    cql3/statements/batch_statement.hh
    cql3/statements/bound.hh
    cql3/statements/cf_prop_defs.cc
    cql3/statements/cf_prop_defs.hh
    cql3/statements/cf_properties.hh
    cql3/statements/cf_statement.cc
    cql3/statements/create_index_statement.cc
    cql3/statements/create_index_statement.hh
    cql3/statements/create_keyspace_statement.cc
    cql3/statements/create_keyspace_statement.hh
    cql3/statements/create_table_statement.cc
    cql3/statements/create_table_statement.hh
    cql3/statements/create_type_statement.cc
    cql3/statements/create_type_statement.hh
    cql3/statements/create_user_statement.cc
    cql3/statements/create_user_statement.hh
    cql3/statements/create_view_statement.cc
    cql3/statements/create_view_statement.hh
    cql3/statements/delete_statement.cc
    cql3/statements/delete_statement.hh
    cql3/statements/drop_index_statement.cc
    cql3/statements/drop_index_statement.hh
    cql3/statements/drop_keyspace_statement.cc
    cql3/statements/drop_keyspace_statement.hh
    cql3/statements/drop_table_statement.cc
    cql3/statements/drop_table_statement.hh
    cql3/statements/drop_type_statement.cc
    cql3/statements/drop_type_statement.hh
    cql3/statements/drop_user_statement.cc
    cql3/statements/drop_user_statement.hh
    cql3/statements/drop_view_statement.cc
    cql3/statements/drop_view_statement.hh
    cql3/statements/grant_statement.cc
    cql3/statements/grant_statement.hh
    cql3/statements/index_prop_defs.cc
    cql3/statements/index_prop_defs.hh
    cql3/statements/index_target.cc
    cql3/statements/index_target.hh
    cql3/statements/ks_prop_defs.cc
    cql3/statements/ks_prop_defs.hh
    cql3/statements/list_permissions_statement.cc
    cql3/statements/list_permissions_statement.hh
    cql3/statements/list_users_statement.cc
    cql3/statements/list_users_statement.hh
    cql3/statements/modification_statement.cc
    cql3/statements/modification_statement.hh
    cql3/statements/parsed_statement.cc
    cql3/statements/permission_altering_statement.cc
    cql3/statements/permission_altering_statement.hh
    cql3/statements/prepared_statement.hh
    cql3/statements/property_definitions.cc
    cql3/statements/property_definitions.hh
    cql3/statements/raw/batch_statement.hh
    cql3/statements/raw/cf_statement.hh
    cql3/statements/raw/delete_statement.hh
    cql3/statements/raw/insert_statement.hh
    cql3/statements/raw/modification_statement.hh
    cql3/statements/raw/parsed_statement.hh
    cql3/statements/raw/select_statement.hh
    cql3/statements/raw/update_statement.hh
    cql3/statements/raw/use_statement.hh
    cql3/statements/request_validations.hh
    cql3/statements/revoke_statement.cc
    cql3/statements/revoke_statement.hh
    cql3/statements/schema_altering_statement.cc
    cql3/statements/schema_altering_statement.hh
    cql3/statements/select_statement.cc
    cql3/statements/select_statement.hh
    cql3/statements/statement_type.hh
    cql3/statements/truncate_statement.cc
    cql3/statements/truncate_statement.hh
    cql3/statements/update_statement.cc
    cql3/statements/update_statement.hh
    cql3/statements/use_statement.cc
    cql3/statements/use_statement.hh
    cql3/stats.hh
    cql3/term.hh
    cql3/token_relation.cc
    cql3/token_relation.hh
    cql3/tuples.hh
    cql3/type_cast.hh
    cql3/untyped_result_set.cc
    cql3/untyped_result_set.hh
    cql3/update_parameters.cc
    cql3/update_parameters.hh
    cql3/user_options.cc
    cql3/user_options.hh
    cql3/user_types.cc
    cql3/user_types.hh
    cql3/ut_name.cc
    cql3/ut_name.hh
    cql3/util.hh
    cql3/values.hh
    cql3/variable_specifications.cc
    cql3/variable_specifications.hh
    cql_serialization_format.hh
    database.cc
    database.hh
    database_fwd.hh
    db/batchlog_manager.cc
    db/batchlog_manager.hh
    db/commitlog/commitlog.cc
    db/commitlog/commitlog.hh
    db/commitlog/commitlog_entry.cc
    db/commitlog/commitlog_entry.hh
    db/commitlog/commitlog_replayer.cc
    db/commitlog/commitlog_replayer.hh
    db/commitlog/replay_position.hh
    db/commitlog/rp_set.hh
    db/config.cc
    db/config.hh
    db/consistency_level.cc
    db/consistency_level.hh
    db/consistency_level_type.hh
    db/cql_type_parser.cc
    db/cql_type_parser.hh
    db/heat_load_balance.cc
    db/heat_load_balance.hh
    db/index/secondary_index.cc
    db/index/secondary_index.hh
    db/legacy_schema_migrator.cc
    db/legacy_schema_migrator.hh
    db/marshal/type_parser.cc
    db/marshal/type_parser.hh
    db/query_context.hh
    db/read_repair_decision.hh
    db/schema_tables.cc
    db/schema_tables.hh
    db/size_estimates_virtual_reader.hh
    db/system_keyspace.cc
    db/system_keyspace.hh
    db/view/view.cc
    db/view/view.hh
    db/write_type.hh
    db_clock.hh
    debug.hh
    dht/boot_strapper.cc
    dht/boot_strapper.hh
    dht/byte_ordered_partitioner.cc
    dht/byte_ordered_partitioner.hh
    dht/i_partitioner.cc
    dht/i_partitioner.hh
    dht/murmur3_partitioner.cc
    dht/murmur3_partitioner.hh
    dht/random_partitioner.cc
    dht/random_partitioner.hh
    dht/range_streamer.cc
    dht/range_streamer.hh
    dht/token_range_endpoints.hh
    digest_algorithm.hh
    dirty_memory_manager.hh
    disk-error-handler.cc
    disk-error-handler.hh
    duration.cc
    duration.hh
    enum_set.hh
    exceptions/exceptions.cc
    exceptions/exceptions.hh
    exceptions/unrecognized_entity_exception.hh
    fnv1a_hasher.hh
    frozen_mutation.cc
    frozen_mutation.hh
    frozen_schema.cc
    frozen_schema.hh
    gc_clock.hh
    gms/application_state.cc
    gms/application_state.hh
    gms/endpoint_state.cc
    gms/endpoint_state.hh
    gms/failure_detector.cc
    gms/failure_detector.hh
    gms/feature.hh
    gms/gossip_digest.hh
    gms/gossip_digest_ack.cc
    gms/gossip_digest_ack.hh
    gms/gossip_digest_ack2.cc
    gms/gossip_digest_ack2.hh
    gms/gossip_digest_syn.cc
    gms/gossip_digest_syn.hh
    gms/gossiper.cc
    gms/gossiper.hh
    gms/heart_beat_state.hh
    gms/i_endpoint_state_change_subscriber.hh
    gms/i_failure_detection_event_listener.hh
    gms/i_failure_detector.hh
    gms/inet_address.cc
    gms/inet_address.hh
    gms/version_generator.cc
    gms/version_generator.hh
    gms/versioned_value.cc
    gms/versioned_value.hh
    hashing.hh
    hashing_partition_visitor.hh
    index/secondary_index_manager.cc
    index/secondary_index_manager.hh
    init.cc
    init.hh
    intrusive_set_external_comparator.hh
    io/i_serializer.hh
    io/i_versioned_serializer.hh
    io/io.cc
    json.hh
    keys.cc
    keys.hh
    lister.cc
    lister.hh
    locator/abstract_replication_strategy.cc
    locator/abstract_replication_strategy.hh
    locator/ec2_multi_region_snitch.cc
    locator/ec2_multi_region_snitch.hh
    locator/ec2_snitch.cc
    locator/ec2_snitch.hh
    locator/everywhere_replication_strategy.cc
    locator/everywhere_replication_strategy.hh
    locator/gossiping_property_file_snitch.cc
    locator/gossiping_property_file_snitch.hh
    locator/local_strategy.cc
    locator/local_strategy.hh
    locator/locator.cc
    locator/network_topology_strategy.cc
    locator/network_topology_strategy.hh
    locator/production_snitch_base.cc
    locator/production_snitch_base.hh
    locator/rack_inferring_snitch.cc
    locator/rack_inferring_snitch.hh
    locator/reconnectable_snitch_helper.hh
    locator/simple_snitch.cc
    locator/simple_snitch.hh
    locator/simple_strategy.cc
    locator/simple_strategy.hh
    locator/snitch_base.cc
    locator/snitch_base.hh
    locator/token_metadata.cc
    locator/token_metadata.hh
    log.hh
    map_difference.hh
    md5_hasher.hh
    memtable-sstable.hh
    memtable.cc
    memtable.hh
    message/messaging_service.cc
    message/messaging_service.hh
    message/messaging_service_fwd.hh
    mutation.cc
    mutation.hh
    mutation_compactor.hh
    mutation_partition.cc
    mutation_partition.hh
    mutation_partition_applier.hh
    mutation_partition_serializer.cc
    mutation_partition_serializer.hh
    mutation_partition_view.cc
    mutation_partition_view.hh
    mutation_partition_visitor.hh
    mutation_query.cc
    mutation_query.hh
    mutation_reader.cc
    mutation_reader.hh
    noexcept_traits.hh
    nway_merger.hh
    partition_builder.hh
    partition_range_compat.hh
    partition_slice_builder.cc
    partition_slice_builder.hh
    partition_snapshot_reader.hh
    partition_snapshot_row_cursor.hh
    partition_version.cc
    partition_version.hh
    position_in_partition.hh
    query-request.hh
    query-result-reader.hh
    query-result-set.cc
    query-result-set.hh
    query-result-writer.hh
    query-result.hh
    query.cc
    query_result_merger.hh
    range.hh
    range_tombstone.cc
    range_tombstone.hh
    range_tombstone_list.cc
    range_tombstone_list.hh
    read_context.hh
    release.cc
    release.hh
    repair/range_split.hh
    repair/repair.cc
    repair/repair.hh
    reversibly_mergeable.hh
    row_cache.cc
    row_cache.hh
    schema.cc
    schema.hh
    schema_builder.hh
    schema_mutations.cc
    schema_mutations.hh
    schema_registry.cc
    schema_registry.hh
    schema_upgrader.hh
    seastarx.hh
    serialization_visitors.hh
    serializer.hh
    serializer_impl.hh
    service/cache_hitrate_calculator.hh
    service/client_state.cc
    service/client_state.hh
    service/endpoint_lifecycle_subscriber.hh
    service/load_broadcaster.hh
    service/migration_listener.hh
    service/migration_manager.cc
    service/migration_manager.hh
    service/migration_task.cc
    service/migration_task.hh
    service/misc_services.cc
    service/pager/paging_state.cc
    service/pager/paging_state.hh
    service/pager/query_pager.hh
    service/pager/query_pagers.cc
    service/pager/query_pagers.hh
    service/priority_manager.cc
    service/priority_manager.hh
    service/query_state.hh
    service/storage_proxy.cc
    service/storage_proxy.hh
    service/storage_service.cc
    service/storage_service.hh
    sstable_mutation_readers.hh
    sstables/atomic_deletion.cc
    sstables/atomic_deletion.hh
    sstables/binary_search.hh
    sstables/column_name_helper.hh
    sstables/compaction.cc
    sstables/compaction.hh
    sstables/compaction_manager.cc
    sstables/compaction_manager.hh
    sstables/compaction_strategy.cc
    sstables/compaction_strategy_impl.hh
    sstables/compress.cc
    sstables/compress.hh
    sstables/consumer.hh
    sstables/date_tiered_compaction_strategy.hh
    sstables/disk_types.hh
    sstables/downsampling.hh
    sstables/exceptions.hh
    sstables/filter.cc
    sstables/filter.hh
    sstables/hyperloglog.hh
    sstables/index_reader.hh
    sstables/key.hh
    sstables/leveled_compaction_strategy.hh
    sstables/leveled_manifest.hh
    sstables/metadata_collector.hh
    sstables/partition.cc
    sstables/remove.hh
    sstables/row.cc
    sstables/row.hh
    sstables/segmented_compress_params.hh
    sstables/shared_index_lists.hh
    sstables/size_tiered_compaction_strategy.hh
    sstables/sstable_set.hh
    sstables/sstables.cc
    sstables/sstables.hh
    sstables/time_window_compaction_strategy.hh
    sstables/types.hh
    sstables/writer.hh
    stdx.hh
    streamed_mutation.cc
    streamed_mutation.hh
    streaming/prepare_message.hh
    streaming/progress_info.cc
    streaming/progress_info.hh
    streaming/session_info.cc
    streaming/session_info.hh
    streaming/stream_coordinator.cc
    streaming/stream_coordinator.hh
    streaming/stream_detail.hh
    streaming/stream_event.hh
    streaming/stream_event_handler.hh
    streaming/stream_exception.hh
    streaming/stream_manager.cc
    streaming/stream_manager.hh
    streaming/stream_plan.cc
    streaming/stream_plan.hh
    streaming/stream_receive_task.cc
    streaming/stream_receive_task.hh
    streaming/stream_request.cc
    streaming/stream_request.hh
    streaming/stream_result_future.cc
    streaming/stream_result_future.hh
    streaming/stream_session.cc
    streaming/stream_session.hh
    streaming/stream_session_state.cc
    streaming/stream_session_state.hh
    streaming/stream_state.hh
    streaming/stream_summary.cc
    streaming/stream_summary.hh
    streaming/stream_task.cc
    streaming/stream_task.hh
    streaming/stream_transfer_task.cc
    streaming/stream_transfer_task.hh
    supervisor.cc
    supervisor.hh
    table_helper.cc
    table_helper.hh
    thrift/handler.cc
    thrift/handler.hh
    thrift/server.cc
    thrift/server.hh
    thrift/thrift_validation.cc
    thrift/thrift_validation.hh
    thrift/utils.hh
    timestamp.hh
    to_string.hh
    tombstone.hh
    tracing/trace_keyspace_helper.cc
    tracing/trace_keyspace_helper.hh
    tracing/trace_state.cc
    tracing/trace_state.hh
    tracing/tracing.cc
    tracing/tracing.hh
    transport/event.cc
    transport/event.hh
    transport/event_notifier.cc
    transport/messages/result_message.hh
    transport/messages/result_message_base.hh
    transport/messages_fwd.hh
    transport/server.cc
    transport/server.hh
    types.cc
    types.hh
    unimplemented.cc
    unimplemented.hh
    utils/UUID.hh
    utils/UUID_gen.cc
    utils/UUID_gen.hh
    utils/allocation_strategy.hh
    utils/anchorless_list.hh
    utils/big_decimal.cc
    utils/big_decimal.hh
    utils/bloom_calculations.cc
    utils/bloom_calculations.hh
    utils/bloom_filter.cc
    utils/bloom_filter.hh
    utils/bounded_stats_deque.hh
    utils/class_registrator.hh
    utils/crc.hh
    utils/data_input.hh
    utils/data_output.hh
    utils/div_ceil.hh
    utils/dynamic_bitset.cc
    utils/dynamic_bitset.hh
    utils/estimated_histogram.hh
    utils/exceptions.cc
    utils/exceptions.hh
    utils/exponential_backoff_retry.hh
    utils/fb_utilities.hh
    utils/file_lock.cc
    utils/file_lock.hh
    utils/flush_queue.hh
    utils/hash.hh
    utils/histogram.hh
    utils/i_filter.cc
    utils/i_filter.hh
    utils/input_stream.hh
    utils/int_range.hh
    utils/joinpoint.hh
    utils/large_bitset.cc
    utils/large_bitset.hh
    utils/latency.hh
    utils/loading_cache.hh
    utils/log_histogram.hh
    utils/logalloc.cc
    utils/logalloc.hh
    utils/managed_bytes.cc
    utils/managed_bytes.hh
    utils/managed_ref.hh
    utils/managed_vector.hh
    utils/move.hh
    utils/murmur_hash.cc
    utils/murmur_hash.hh
    utils/mutable_view.hh
    utils/optimized_optional.hh
    utils/phased_barrier.hh
    utils/rate_limiter.cc
    utils/rate_limiter.hh
    utils/runtime.cc
    utils/runtime.hh
    utils/sequenced_set.hh
    utils/serialization.hh
    utils/serialized_action.hh
    utils/streaming_histogram.hh
    utils/to_boost_visitor.hh
    utils/utils.cc
    utils/uuid.cc
    utils/with_relational_operators.hh
    validation.cc
    validation.hh
    version.hh
    view_info.hh
    vint-serialization.cc
    vint-serialization.hh
# API
    api/api-doc/cache_service.json
    api/api-doc/collectd.json
    api/api-doc/column_family.json
    api/api-doc/commitlog.json
    api/api-doc/compaction_manager.json
    api/api-doc/endpoint_snitch_info.json
    api/api-doc/failure_detector.json
    api/api-doc/gossiper.json
    api/api-doc/hinted_handoff.json
    api/api-doc/lsa.json
    api/api-doc/messaging_service.json
    api/api-doc/storage_proxy.json
    api/api-doc/storage_service.json
    api/api-doc/stream_manager.json
    api/api-doc/system.json
    api/api-doc/utils.json
    api/api.cc
    api/api.hh
    api/api_init.hh
    api/cache_service.cc
    api/cache_service.hh
    api/collectd.cc
    api/collectd.hh
    api/column_family.cc
    api/column_family.hh
    api/commitlog.cc
    api/commitlog.hh
    api/compaction_manager.cc
    api/compaction_manager.hh
    api/endpoint_snitch.cc
    api/endpoint_snitch.hh
    api/failure_detector.cc
    api/failure_detector.hh
    api/gossiper.cc
    api/gossiper.hh
    api/hinted_handoff.cc
    api/hinted_handoff.hh
    api/lsa.cc
    api/lsa.hh
    api/messaging_service.cc
    api/messaging_service.hh
    api/storage_proxy.cc
    api/storage_proxy.hh
    api/storage_service.cc
    api/storage_service.hh
    api/stream_manager.cc
    api/stream_manager.hh
    api/system.cc
    api/system.hh
# IDL
    idl/cache_temperature.idl.hh
    idl/commitlog.idl.hh
    idl/consistency_level.idl.hh
    idl/frozen_mutation.idl.hh
    idl/frozen_schema.idl.hh
    idl/gossip_digest.idl.hh
    idl/idl_test.idl.hh
    idl/keys.idl.hh
    idl/mutation.idl.hh
    idl/paging_state.idl.hh
    idl/partition_checksum.idl.hh
    idl/query.idl.hh
    idl/range.idl.hh
    idl/read_command.idl.hh
    idl/reconcilable_result.idl.hh
    idl/replay_position.idl.hh
    idl/result.idl.hh
    idl/ring_position.idl.hh
    idl/streaming.idl.hh
    idl/token.idl.hh
    idl/tracing.idl.hh
    idl/truncation_record.idl.hh
    idl/uuid.idl.hh
# CQL3
    cql3/Cql.g
# Thrift
    interface/cassandra.thrift
)


##
## Functions
##


function(process_idl_source source processed_idl_source)
    get_filename_component(idl_input_header_file ${source} NAME)

    string(REGEX REPLACE "(.*)\.idl\.(.*)" "\\1.dist.\\2" idl_output_header_file ${idl_input_header_file})

    set(idl_output_header ${GEN_DIR}/idl/${idl_output_header_file})
    add_custom_command(
        OUTPUT ${idl_output_header}
        COMMAND python3 ${CMAKE_SOURCE_DIR}/idl-compiler.py --ns ser -f ${CMAKE_SOURCE_DIR}/${source} -o ${idl_output_header}
        DEPENDS ${source}
    )

    set(${processed_idl_source} ${idl_output_header} PARENT_SCOPE)
endfunction(process_idl_source)


function(process_api_json_source source processed_api_json_source)
    get_filename_component(api_doc_input_json_file ${source} NAME)

    set(api_doc_output_header_file "${api_doc_input_json_file}.hh")
    set(api_doc_output_header ${GEN_DIR}/api/api-doc/${api_doc_output_header_file})
    add_custom_command(
        OUTPUT ${api_doc_output_header}
        #TODO use some imported target instead of hardcoded path, once seastar CMake is ready
        COMMAND python3 ${CMAKE_SOURCE_DIR}/seastar/json/json2code.py -f ${CMAKE_SOURCE_DIR}/${source} -o ${api_doc_output_header}
        DEPENDS ${source}
    )

    set(${processed_api_json_source} ${api_doc_output_header} PARENT_SCOPE)
endfunction(process_api_json_source)


function(generate_cql_grammar grammar_in grammar_out)
    set(
        _grammar_out
        ${GEN_DIR}/cql3/CqlLexer.cpp
        ${GEN_DIR}/cql3/CqlLexer.hpp
        ${GEN_DIR}/cql3/CqlParser.cpp
        ${GEN_DIR}/cql3/CqlParser.hpp
    )

    add_custom_command(
        OUTPUT
        ${_grammar_out}
        COMMAND ${CMAKE_SOURCE_DIR}/antlr3.sh ${CMAKE_SOURCE_DIR} ${GEN_DIR}
        DEPENDS ${grammar_in}
    )

    set(${grammar_out} ${_grammar_out} PARENT_SCOPE)
endfunction(generate_cql_grammar)


function(generate_thrift thrift_in thrift_out)
    set(
        _thrift_out
        ${GEN_DIR}/cassandra_constants.cpp
        ${GEN_DIR}/cassandra_constants.h
        ${GEN_DIR}/Cassandra.cpp
        ${GEN_DIR}/Cassandra.h
        ${GEN_DIR}/cassandra_types.cpp
        ${GEN_DIR}/cassandra_types.h
    )

    add_custom_command(
        OUTPUT
        ${_thrift_out}
        COMMAND thrift -gen cpp:cob_style -out ${GEN_DIR} ${CMAKE_SOURCE_DIR}/interface/cassandra.thrift
        DEPENDS ${thrift_in}
    )

    set(${thrift_out} ${_thrift_out} PARENT_SCOPE)
endfunction(generate_thrift)


function(add_version_defines version_source)
    set(VERSION_FILE ${CMAKE_SOURCE_DIR}/build/SCYLLA-VERSION-FILE)
    set(RELEASE_FILE ${CMAKE_SOURCE_DIR}/build/SCYLLA-RELEASE-FILE)

    execute_process(
        COMMAND ${CMAKE_SOURCE_DIR}/SCYLLA-VERSION-GEN
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )

    file(STRINGS ${VERSION_FILE} SCYLLA_VERSION)
    file(STRINGS ${RELEASE_FILE} SCYLLA_RELEASE)

    set_source_files_properties(
        ${version_source}
        COMPILE_FLAGS
            "-DSCYLLA_VERSION=\"\\\"${SCYLLA_VERSION}\\\"\" -DSCYLLA_RELEASE=\"\\\"${SCYLLA_RELEASE}\\\"\""
    )
endfunction(add_version_defines)


#
# Process a source file.
#
# In the following way:
# * C++ source (*.cc) -> no processing
# * C++ header (*.hh) -> no processing
# * IDL header (idl/*.hh) -> generate dist header
# * API doc json (api/api-doc/*.json) -> generate doc header
# * ANTLR3 (cql3/Cql.g) -> generate grammar files
# * THRIFT (interface/cassandra.thrift) -> generate thrift files
# * VERSION (release.cc) -> add version defines
#
# The resulting source file(s) are put in ${processed_source}
#
function(process_source source processed_source)
    string(REGEX MATCH "^idl/.*\.idl\.hh$" is_idl ${source})
    string(REGEX MATCH "^api/api-doc/.*\.json$" is_api_json ${source})
    string(REGEX MATCH "^cql3/Cql.g$" is_antlr ${source})
    string(REGEX MATCH "^interface/cassandra.thrift$" is_thrift ${source})
    string(REGEX MATCH "^release.cc$" is_version ${source})

    if(is_idl)
        process_idl_source(${source} processed_idl_source)
        set(${processed_source} ${processed_idl_source} PARENT_SCOPE)
    elseif(is_api_json)
        process_api_json_source(${source} processed_api_json_source)
        set(${processed_source} ${processed_api_json_source} PARENT_SCOPE)
    elseif(is_antlr)
        generate_cql_grammar(${source} generated_grammar_files)
        set(${processed_source} ${generated_grammar_files} PARENT_SCOPE)
    elseif(is_thrift)
        generate_thrift(${source} generated_thrift_files)
        set(${processed_source} ${generated_thrift_files} PARENT_SCOPE)
    elseif(is_version)
        add_version_defines(${source})
        set(${processed_source} ${source} PARENT_SCOPE)
    else() # Only C++ sources (.hh, .cc) should fall-back here
        set(${processed_source} ${source} PARENT_SCOPE)
    endif()
endfunction(process_source)


function(process_sources sources processed_sources)
    set(_processed_source_list "")

    foreach(src ${sources})
        process_source(${src} _processed_srcs)
        list(APPEND _processed_source_list "${_processed_srcs}")
    endforeach()

    set(${processed_sources} ${_processed_source_list} PARENT_SCOPE)
endfunction(process_sources)


# The test_name param should map to a source file in tests, such that
# tests/${test_name}.cc is a valid source file.
function(add_scylla_test test_name)
    set(multiValueArgs ADDITIONAL_SOURCES)
    cmake_parse_arguments("add_scylla_test" "" "" "${multiValueArgs}" ${ARGN})

    option("with_${test_name}" "Build ${test_name} test?" ON)

    if("with_${test_name}" OR with_all_tests OR with_all)
        add_executable(
            ${test_name}
            "tests/${test_name}.cc"
            "seastar/tests/test-utils.cc"
            "seastar/tests/test_runner.cc"
            "tests/cql_test_env.cc"
            "tests/cql_assertions.cc"
            "tests/result_set_assertions.cc"
            "tests/mutation_source_test.cc"
        )

        target_link_libraries(
            ${test_name}
            ${TEST_LIBRARIES}
            scylla_blob
        )
    endif()
endfunction(add_scylla_test)


function(find_supported_warnings warning_blacklist supported_warnings_var)
    set(_supported_warnings "")
    file(WRITE "${CMAKE_BINARY_DIR}/warning_test.cc" "int main(){ return 0; }")

    foreach(_warning ${warning_blacklist})
        string(REGEX REPLACE "^-Wno-" "-W" _warning_enable_form ${_warning})

        set(_warning_supported NO)
        #check_cxx_compiler_flag("${_warning_enable_form}" _warning_supported) -> just doesn't work
        try_compile(_warning_supported ${CMAKE_BINARY_DIR} "${CMAKE_BINARY_DIR}/warning_test.cc" COMPILE_DEFINITIONS "${_warning_enable_form}")

        if(_warning_supported)
            list(APPEND _supported_warnings "${_warning}")
        endif()
    endforeach()

    set(${supported_warnings_var} "${_supported_warnings}" PARENT_SCOPE)
    file(REMOVE "${CMAKE_BINARY_DIR}/warning_test.cc")
endfunction(find_supported_warnings)


##
## Configure CMake
##


message("---------------------------------------------------------------------------------------------------")
message("-- Building scylla with CMake is experimental, the supported way of building is via configure.py --")
message("---------------------------------------------------------------------------------------------------")


set(
    warning_blacklist
    -Wno-mismatched-tags  # clang-only
    -Wno-maybe-uninitialized # false positives on gcc 5
    -Wno-tautological-compare
    -Wno-parentheses-equality
    -Wno-c++11-narrowing
    -Wno-c++1z-extensions
    -Wno-sometimes-uninitialized
    -Wno-return-stack-address
    -Wno-missing-braces
    -Wno-unused-lambda-capture
    -Wno-unused-but-set-variable # annoying warnings on variables only used in assert()
    -Wno-unused-variable # annoying warnings on variables only used in assert()
)
find_supported_warnings("${warning_blacklist}" supported_warnings)

set(CMAKE_CXX_STANDARD 14)

#
# Build type
#
# Possible values:
# * debug
# * release
# * relwithdebinfo
# * minsizerel
#
if(CMAKE_BUILD_TYPE)
    string(TOLOWER ${CMAKE_BUILD_TYPE} build_type_lower)
else()
    # Default build-type to release
    set(CMAKE_BUILD_TYPE "release")
    set(build_type_lower "release")
endif()

find_program(
    ninja_executable
    NAMES ninja ninja-build
    DOC "Ninja executable"
)

# Generate seastar.pc if it doesn't exists
if (NOT EXISTS "seastar/build/${build_type_lower}/seastar.pc")
    message(STATUS "Generating seastar package-config in ${CMAKE_SOURCE_DIR}/seastar")
    execute_process(
        COMMAND
            "python3"
            "${CMAKE_SOURCE_DIR}/seastar/configure.py"
            "--mode=${build_type_lower}"
        WORKING_DIRECTORY
            "${CMAKE_SOURCE_DIR}/seastar"
    )

    execute_process(
        COMMAND
            ${ninja_executable}
            "build/${build_type_lower}/seastar.pc"
        WORKING_DIRECTORY
            "${CMAKE_SOURCE_DIR}/seastar"
    )
endif()

find_package(PkgConfig REQUIRED)

set(ENV{PKG_CONFIG_PATH} "${CMAKE_SOURCE_DIR}/seastar/build/${build_type_lower}:$ENV{PKG_CONFIG_PATH}")

pkg_check_modules(SEASTAR seastar)
pkg_check_modules(JSONCPP jsoncpp)

# Prepare code generation directories
set(GEN_DIR ${PROJECT_BINARY_DIR}/gen)
execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${GEN_DIR})
execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${GEN_DIR}/cql3)
execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${GEN_DIR}/idl)

# Prepare source files
process_sources("${SCYLLA_SOURCES}" SCYLLA_PROCESSED_SOURCES)


#
# Targets
#


# Always rebuild seastar
add_custom_target(
    seastar
    COMMAND ${ninja_executable} "build/${build_type_lower}/libseastar.a"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/seastar
    COMMENT "Building libseastar.a"
)


add_library(
    scylla_blob
    STATIC
    ${SCYLLA_PROCESSED_SOURCES}
)

add_dependencies(scylla_blob seastar)

target_include_directories(
    scylla_blob
    PUBLIC
        ${CMAKE_SOURCE_DIR}
        ${GEN_DIR}
        ${SEASTAR_INCLUDE_DIRS}
        ${JSONCPP_INCLUDE_DIRS}
)

target_compile_options(
    scylla_blob
    PUBLIC
        -march=nehalem
        -Wall
        -Werror
        -Wno-error=deprecated-declarations
        -fvisibility=hidden
        -Wno-attributes
        ${supported_warnings}
        ${SEASTAR_CFLAGS}
)

target_link_libraries(
    scylla_blob
    PUBLIC
        ${SEASTAR_LDFLAGS}
        ${JSONCPP_LIBRARIES}
        -lsnappy
        -lyaml-cpp
        -lthrift
        -lcrypt
)

add_executable(
    scylla
    main.cc
)

target_link_libraries(
    scylla
    PRIVATE scylla_blob
)


##
## Test targets
##

find_package(Boost 1.55 REQUIRED COMPONENTS unit_test_framework)

set(
    TEST_LIBRARIES
    Boost_test_LIBRARY
)

add_scylla_test("mutation_test")
add_scylla_test("streamed_mutation_test")
add_scylla_test("schema_registry_test")
add_scylla_test("canonical_mutation_test")
add_scylla_test("range_test")
add_scylla_test("types_test")
add_scylla_test("keys_test")
add_scylla_test("partitioner_test")
add_scylla_test("frozen_mutation_test")
add_scylla_test("serialized_action_test")
add_scylla_test("perf/perf_mutation")
add_scylla_test("lsa_async_eviction_test")
add_scylla_test("lsa_sync_eviction_test")
add_scylla_test("row_cache_alloc_stress")
add_scylla_test("perf_row_cache_update")
add_scylla_test("perf/perf_hash")
add_scylla_test("perf/perf_cql_parser")
add_scylla_test("perf/perf_simple_query")
add_scylla_test("perf/perf_fast_forward")
add_scylla_test("perf/perf_cache_eviction")
add_scylla_test("cache_streamed_mutation_test")
add_scylla_test("row_cache_stress_test")
add_scylla_test("memory_footprint")
add_scylla_test("perf/perf_sstable")
add_scylla_test("cql_query_test")
add_scylla_test("storage_proxy_test")
add_scylla_test("schema_change_test")
add_scylla_test("mutation_reader_test")
add_scylla_test("mutation_query_test")
add_scylla_test("row_cache_test")
add_scylla_test("test-serialization")
add_scylla_test("sstable_test" ADDITIONAL_SOURCES "tests/sstable_datafile_test.cc" "tests/sstable_utils.cc")
add_scylla_test("sstable_mutation_test")
add_scylla_test("sstable_resharding_test")
add_scylla_test("combined_mutation_reader_test" ADDITIONAL_SOURCES "tests/sstable_utils.cc")
add_scylla_test("memtable_test")
add_scylla_test("commitlog_test")
add_scylla_test("cartesian_product_test")
add_scylla_test("hash_test")
add_scylla_test("map_difference_test")
add_scylla_test("message")
add_scylla_test("gossip")
add_scylla_test("gossip_test")
add_scylla_test("compound_test")
add_scylla_test("config_test")
add_scylla_test("gossiping_property_file_snitch_test")
add_scylla_test("ec2_snitch_test")
add_scylla_test("snitch_reset_test")
add_scylla_test("network_topology_strategy_test")
add_scylla_test("query_processor_test")
add_scylla_test("batchlog_manager_test")
add_scylla_test("bytes_ostream_test" ADDITIONAL_SOURCES "utils/managed_bytes.cc" "utils/logalloc.cc" "utils/dynamic_bitset.cc")
add_scylla_test("UUID_test" ADDITIONAL_SOURCES "utils/UUID_gen.cc" "utils/uuid.cc" "utils/managed_bytes.cc" "utils/logalloc.cc" "utils/dynamic_bitset.cc")
add_scylla_test("murmur_hash_test" ADDITIONAL_SOURCES "bytes.cc" "utils/murmur_hash.cc")
add_scylla_test("allocation_strategy_test" ADDITIONAL_SOURCES "utils/logalloc.cc" "utils/dynamic_bitset.cc")
add_scylla_test("logalloc_test")
add_scylla_test("log_histogram_test")
add_scylla_test("managed_vector_test")
add_scylla_test("crc_test")
add_scylla_test("flush_queue_test")
add_scylla_test("dynamic_bitset_test")
add_scylla_test("auth_test")
add_scylla_test("idl_test")
add_scylla_test("range_tombstone_list_test")
add_scylla_test("anchorless_list_test")
add_scylla_test("database_test")
add_scylla_test("nonwrapping_range_test")
add_scylla_test("input_stream_test")
add_scylla_test("sstable_atomic_deletion_test")
add_scylla_test("virtual_reader_test")
add_scylla_test("view_schema_test")
add_scylla_test("counter_test")
add_scylla_test("cell_locker_test")
add_scylla_test("streaming_histogram_test")
add_scylla_test("duration_test")
add_scylla_test("vint_serialization_test")
add_scylla_test("compress_test")

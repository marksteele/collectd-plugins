package Collectd::Plugin::MySQL;
use Collectd qw(:all);
use DBD::mysql;
require InnoDBParser;

=head1 NAME

Collectd::Plugins::MySQL - Monitor a mysql server

=head1 VERSION

Version 1

=cut

our $VERSION = '1';


=head1 SYNOPSIS

This is a collectd plugin for monitoring a mysql server

In your collectd config:

    <LoadPlugin "perl">
       Globals true
    </LoadPlugin>

    <Plugin "perl">
      BaseName "Collectd::Plugins"
      LoadPlugin "MySQL"

     <Plugin "MySQL">
       Host "localhost"
       Port "3306"
       User "root"
       Pass "mypass"
      </Plugin>
    </Plugin>

=head1 AUTHOR

Mark Steele, C<< <mark at control-alt-del.org> >>
    
=cut

my $host = 'localhost';
my $port = 3306;
my $user = 'root';
my $pass = '';

my %keys = ();
my @status_keys = qw(
    aborted_clients aborted_connects binlog_cache_disk_use binlog_cache_use binlog_commits binlog_group_commits binlog_stmt_cache_disk_use binlog_stmt_cache_use bytes_received bytes_sent 
    com_admin_commands com_alter_db com_alter_db_upgrade com_alter_event com_alter_function com_alter_procedure com_alter_server com_alter_table com_alter_tablespace com_analyze com_assign_to_keycache 
    com_begin com_binlog com_call_procedure com_change_db com_change_master com_check com_checksum com_commit com_create_db com_create_event com_create_function com_create_index com_create_procedure 
    com_create_server com_create_table com_create_trigger com_create_udf com_create_user com_create_view com_dealloc_sql com_delete com_delete_multi com_do com_drop_db com_drop_event com_drop_function 
    com_drop_index com_drop_procedure com_drop_server com_drop_table com_drop_trigger com_drop_user com_drop_view com_empty_query com_execute_sql com_flush com_grant com_ha_close com_ha_open com_ha_read 
    com_help com_insert com_insert_select com_install_plugin com_kill com_load com_lock_tables com_optimize com_preload_keys com_prepare_sql com_purge com_purge_before_date com_release_savepoint 
    com_rename_table com_rename_user com_repair com_replace com_replace_select com_reset com_resignal com_revoke com_revoke_all com_rollback com_rollback_to_savepoint com_savepoint com_select com_set_option 
    com_show_authors com_show_binlog_events com_show_binlogs com_show_charsets com_show_client_statistics com_show_collations com_show_contributors com_show_create_db com_show_create_event 
    com_show_create_func com_show_create_proc com_show_create_table com_show_create_trigger com_show_databases com_show_engine_logs com_show_engine_mutex com_show_engine_status com_show_errors 
    com_show_events com_show_fields com_show_function_status com_show_grants com_show_index_statistics com_show_keys com_show_master_status com_show_open_tables com_show_plugins com_show_privileges 
    com_show_procedure_status com_show_processlist com_show_profile com_show_profiles com_show_relaylog_events com_show_slave_hosts com_show_slave_status com_show_slave_status_nolock com_show_status 
    com_show_storage_engines com_show_table_statistics com_show_table_status com_show_tables com_show_temporary_tables com_show_thread_statistics com_show_triggers com_show_user_statistics 
    com_show_variables com_show_warnings com_signal com_slave_start com_slave_stop com_stmt_close com_stmt_execute com_stmt_fetch com_stmt_prepare com_stmt_reprepare com_stmt_reset com_stmt_send_long_data 
    com_truncate com_uninstall_plugin com_unlock_tables com_update com_update_multi com_xa_commit com_xa_end com_xa_prepare com_xa_recover com_xa_rollback com_xa_start compression connections 
    created_tmp_disk_tables created_tmp_files created_tmp_tables delayed_errors delayed_insert_threads delayed_writes flashcache_enabled flush_commands handler_commit handler_delete handler_discover 
    handler_prepare handler_read_first handler_read_key handler_read_last handler_read_next handler_read_prev handler_read_rnd handler_read_rnd_next handler_rollback handler_savepoint 
    handler_savepoint_rollback handler_update handler_write innodb_adaptive_hash_cells innodb_adaptive_hash_hash_searches innodb_adaptive_hash_heap_buffers innodb_adaptive_hash_non_hash_searches 
    innodb_background_log_sync innodb_buffer_pool_pages_lru_flushed innodb_buffer_pool_pages_data innodb_buffer_pool_pages_dirty innodb_buffer_pool_pages_flushed innodb_buffer_pool_pages_free 
    innodb_buffer_pool_pages_made_not_young innodb_buffer_pool_pages_made_young innodb_buffer_pool_pages_misc innodb_buffer_pool_pages_old innodb_buffer_pool_pages_total innodb_buffer_pool_read_ahead 
    innodb_buffer_pool_read_ahead_evicted innodb_buffer_pool_read_ahead_rnd innodb_buffer_pool_read_requests innodb_buffer_pool_reads innodb_buffer_pool_wait_free innodb_buffer_pool_write_requests 
    innodb_checkpoint_age innodb_checkpoint_max_age innodb_checkpoint_target_age innodb_current_row_locks innodb_data_fsyncs innodb_data_pending_fsyncs innodb_data_pending_reads innodb_data_pending_writes 
    innodb_data_read innodb_data_reads innodb_data_writes innodb_data_written innodb_dblwr_pages_written innodb_dblwr_writes innodb_deadlocks innodb_dict_tables innodb_have_atomic_builtins 
    innodb_history_list_length innodb_ibuf_discarded_delete_marks innodb_ibuf_discarded_deletes innodb_ibuf_discarded_inserts innodb_ibuf_free_list innodb_ibuf_merged_delete_marks innodb_ibuf_merged_deletes 
    innodb_ibuf_merged_inserts innodb_ibuf_merges innodb_ibuf_segment_size innodb_ibuf_size innodb_log_waits innodb_log_write_requests innodb_log_writes innodb_lsn_current innodb_lsn_flushed 
    innodb_lsn_last_checkpoint innodb_master_thread_10_second_loops innodb_master_thread_1_second_loops innodb_master_thread_background_loops innodb_master_thread_main_flush_loops 
    innodb_master_thread_sleeps innodb_max_trx_id innodb_mem_adaptive_hash innodb_mem_dictionary innodb_mem_total innodb_mutex_os_waits innodb_mutex_spin_rounds innodb_mutex_spin_waits 
    innodb_oldest_view_low_limit_trx_id innodb_os_log_fsyncs innodb_os_log_pending_fsyncs innodb_os_log_pending_writes innodb_os_log_written innodb_page_size innodb_pages_created innodb_pages_read 
    innodb_pages_written innodb_purge_trx_id innodb_purge_undo_no innodb_row_lock_current_waits innodb_row_lock_time innodb_row_lock_time_avg innodb_row_lock_time_max innodb_row_lock_waits 
    innodb_rows_deleted innodb_rows_inserted innodb_rows_read innodb_rows_updated innodb_s_lock_os_waits innodb_s_lock_spin_rounds innodb_s_lock_spin_waits innodb_truncated_status_writes 
    innodb_x_lock_os_waits innodb_x_lock_spin_rounds innodb_x_lock_spin_waits key_blocks_not_flushed key_blocks_unused key_blocks_used key_read_requests key_reads key_write_requests key_writes 
    last_query_cost max_used_connections not_flushed_delayed_rows open_files open_streams open_table_definitions open_tables opened_files opened_table_definitions opened_tables 
    performance_schema_cond_classes_lost performance_schema_cond_instances_lost performance_schema_file_classes_lost performance_schema_file_handles_lost performance_schema_file_instances_lost 
    performance_schema_locker_lost performance_schema_mutex_classes_lost performance_schema_mutex_instances_lost performance_schema_rwlock_classes_lost performance_schema_rwlock_instances_lost 
    performance_schema_table_handles_lost performance_schema_table_instances_lost performance_schema_thread_classes_lost performance_schema_thread_instances_lost prepared_stmt_count qcache_free_blocks 
    qcache_free_memory qcache_hits qcache_inserts qcache_lowmem_prunes qcache_not_cached qcache_queries_in_cache qcache_total_blocks queries questions rpl_status select_full_join select_full_range_join 
    select_range select_range_check select_scan slave_heartbeat_period slave_open_temp_tables slave_received_heartbeats slave_retried_transactions slave_running slow_launch_threads slow_queries 
    sort_merge_passes sort_range sort_rows sort_scan ssl_accept_renegotiates ssl_accepts ssl_callback_cache_hits ssl_cipher ssl_cipher_list ssl_client_connects ssl_connect_renegotiates ssl_ctx_verify_depth 
    ssl_ctx_verify_mode ssl_default_timeout ssl_finished_accepts ssl_finished_connects ssl_session_cache_hits ssl_session_cache_misses ssl_session_cache_mode ssl_session_cache_overflows 
    ssl_session_cache_size ssl_session_cache_timeouts ssl_sessions_reused ssl_used_session_cache_entries ssl_verify_depth ssl_verify_mode ssl_version table_locks_immediate table_locks_waited 
    tc_log_max_pages_used tc_log_page_size tc_log_page_waits threads_cached threads_connected threads_created threads_running uptime uptime_since_flush_status 
    wsrep_apply_oooe wsrep_apply_oool wsrep_causal_reads wsrep_commit_oooe wsrep_commit_oool wsrep_flow_control_recv wsrep_flow_control_sent wsrep_local_bf_aborts
    wsrep_local_commits wsrep_local_replays wsrep_received wsrep_received_bytes wsrep_replicated wsrep_replicated_bytes 
    wsrep_apply_window wsrep_cert_deps_distance wsrep_local_recv_queue wsrep_local_recv_queue_avg wsrep_local_send_queue wsrep_local_send_queue_avg wsrep_commit_window
    wsrep_local_cert_failures wsrep_cert_index_size wsrep_local_state wsrep_flow_control_paused wsrep_cluster_size wsrep_last_committed
    innodb_buffer_pool_bytes_data threadpool_idle_threads wsrep_thread_count innodb_descriptors_memory innodb_read_views_memory innodb_buffer_pool_bytes_dirty
    threadpool_threads
); 


my @slave_keys = qw(exec_master_log_pos read_master_log_pos seconds_behind_master slave_io_running slave_sql_running);

my @innodb_bp_keys = qw(
      add_pool_alloc awe_mem_alloc buf_free buf_pool_hits buf_pool_reads buf_pool_size dict_mem_alloc page_creates_sec page_reads_sec page_writes_sec pages_created pages_modified 
      pages_read pages_total pages_written reads_pending total_mem_alloc writes_pending writes_pending_flush_list writes_pending_lru writes_pending_single_page
); 

my @innodb_ib_keys = qw(
    bufs_in_node_heap free_list_len hash_searches_s hash_table_size inserts merged_recs merges non_hash_searches_s seg_size size used_cells
); 

my @innodb_io_keys = qw(
    avg_bytes_s flush_type fsyncs_s os_file_reads os_file_writes os_fsyncs pending_aio_writes pending_buffer_pool_flushes pending_ibuf_aio_reads pending_log_flushes pending_log_ios 
    pending_normal_aio_reads pending_preads pending_pwrites pending_sync_ios reads_s writes_s
);

my @innodb_lg_keys = qw(last_chkp log_flushed_to log_ios_done log_ios_s log_seq_no pending_chkp_writes pending_log_writes);

my @innodb_ro_keys = qw(del_sec ins_sec n_reserved_extents num_rows_del num_rows_ins num_rows_read num_rows_upd queries_in_queue queries_inside read_sec read_views_open upd_sec);

my @innodb_sm_keys = qw(mutex_os_waits mutex_spin_rounds mutex_spin_waits reservation_count rw_excl_os_waits rw_excl_spins rw_shared_os_waits rw_shared_spins signal_count wait_array_size);

my @pstate_keys = qw(
  after_create analyzing checking_permissions checking_table cleaning_up closing_tables converting_heap_to_myisam copy_to_tmp_table copying_to_tmp_table_on_disk creating_index creating_sort_index
  copying_to_group_table creating_table creating_tmp_table deleting_from_main_table deleting_from_reference_table discard_or_import_tablespace end executing execution_of_init_command freeing_items
  flushing_tables fulltext_initialization init killed locked logging_slow_query null manage_keys opening_table optimizing preparing purging_old_relay_logs query_end reading_from_net removing_duplicates
  removing_tmp_table rename rename_result_table reopen_tables repair_by_sorting repair_done repair_with_keycache rolling_back 
  saving_state searching_rows_for_update sending_data setup sleep sorting_for_group sorting_for_order sorting_index sorting_result statistics system_lock 
  updating updating_main_table updating_reference_tables user_lock user_
  waiting_for_table waiting_on_cond writing_to_net wsrep wsrep_commit wsrep_write_row
  other
);

@{$keys{'status'}}{@status_keys} = undef;
@{$keys{'slave'}}{@slave_keys} = undef; 
@{$keys{'innodb'}{'bp'}}{@innodb_bp_keys} = undef;
@{$keys{'innodb'}{'ib'}}{@innodb_ib_keys} = undef;
@{$keys{'innodb'}{'io'}}{@innodb_io_keys} = undef;
@{$keys{'innodb'}{'lg'}}{@innodb_lg_keys} = undef;
@{$keys{'innodb'}{'ro'}}{@innodb_ro_keys} = undef;
@{$keys{'innodb'}{'sm'}}{@innodb_sm_keys} = undef;
@{$keys{'pstate'}}{@pstate_keys} = undef; 

plugin_register (TYPE_READ, 'MySQL', 'my_read');
plugin_register (TYPE_CONFIG, 'MySQL', 'mysql_config');

sub mysql_config {
  my ($ci) = @_;
  foreach my $item (@{$ci->{'children'}}) {
    my $key = lc($item->{'key'});
    my $val = $item->{'values'}->[0];
    if ($key eq 'host' ) {
      $host = $val;
     } elsif ($key eq 'port' ) {
       $port = $val;
     } elsif ($key eq 'user') {
        $user = $val;
     } elsif ($key eq 'pass') {
         $pass = $val;
     }
  }
  return 1;
}

# Support function. Reads and returns a single configuration value from MySQL itself.
sub read_mysql_variable {
  my ( $dbh, $varname ) = @_;
  my $value = ( $dbh->selectrow_array( qq{SHOW /*!40003 GLOBAL*/ VARIABLES LIKE "\Q$varname\E"} ) )[1];
  return $value;
}

# Support function. Reads and returns the PID of MySQL from a given filename.
sub read_mysql_pid_from_file {
  my $pid_file = shift;
  open( my $fh, '<', $pid_file ) or die qq{Cannot open '$pid_file' for reading: $!};
  my $pid = readline $fh;
  close $fh or die qq{Cannot close '$pid_file' after reading: $!};
  chomp $pid;
  return $pid;
}

# Support function. Calculates and returns the name of the "innodb_status" file
# from MySQL's configuration.
sub innodb_status_filename {
  my $dbh = shift;
  my $mysql_datadir          = read_mysql_variable $dbh, 'datadir';
  my $mysql_pidfile          = read_mysql_variable $dbh, 'pid_file';
  my $mysql_pid              = read_mysql_pid_from_file $mysql_pidfile;
  my $innodb_status_filename = qq{$mysql_datadir/innodb_status.$mysql_pid};
  return $innodb_status_filename;
}

# Support function. Reads innodb status from either the dump-file
# (${mysql::datadir}/innodb_status.${mysql::pid}) or
# 'SHOW ENGINE INNODB STATUS'. It prefers the file to the SQL to
# avoid the 64KB limitation on the SQL wherever possible.
sub read_innodb_status_from_file_or_sql {
  my $dbh                     = shift;
  my $innodb_status_filename  = innodb_status_filename $dbh;
  my $innodb_status_fulltext;
  if( -r $innodb_status_filename ){
    open my $fh, '<', $innodb_status_filename
      or die qq{Cannot open innodb status file '$innodb_status_filename' for reading: $!};
    $innodb_status_fulltext = do{ local $/ = undef; <$fh> };
    close $fh
      or die qq{CAnnot close innodb status file '$innodb_status_filename' after reading: $!};
  } else {
    $innodb_status_fulltext = ${ $dbh->selectrow_hashref( q{SHOW /*!50000 ENGINE*/ INNODB STATUS} ) }{Status};
    # my @result = $dbh->selectrow_array( q{SHOW /*!50000 ENGINE*/ INNODB STATUS} );
    # $innodb_status_fulltext = $result[1];
  }

  return $innodb_status_fulltext;
}


sub my_read {
  my $dbh = DBI->connect("DBI:mysql:database=mysql;host=$host;port=$port", $user, $pass) || return 0;
  my $status = $dbh->selectall_hashref("SHOW /*!50002 GLOBAL */ STATUS",'Variable_name');
  $status = { map { lc($_) => $status->{$_}} keys %{$status}};
  my $slave = $dbh->selectrow_hashref("SHOW SLAVE STATUS");
  $slave = {map { lc($_) => $slave->{$_}} keys %{$slave}};
  my $sql = 'SELECT VERSION();';
  my ($mysqlver) = $dbh->selectrow_array($sql);
  my $parser = InnoDBParser->new;
  my $innodb_status = $parser->parse_status_text(read_innodb_status_from_file_or_sql( $dbh ), 0, undef, undef, $mysqlver);
  my $plist = $dbh->selectall_arrayref("SHOW PROCESSLIST", { Slice => {}});
  $dbh->disconnect();

  my %states;
  foreach my $item (@{$plist}) {
    if ($item->{'State'}) {
      my $pstate = lc($item->{'State'});
      $pstate =~ s/^(?:table lock|taiting.*lock)$/locked/;
      $pstate =~ s/^opening tables/opening table/;
      $pstate =~ s/^waiting for tables/waiting_for_table/;
      $pstate =~ s/^Sleeping.*/sleep/i;
      $pstate =~ s/^update.*/updating/i;
      $pstate =~ s/^write_rows_log_event__write_row.*/wsrep_write_row/i;
      $pstate =~ s/^committed_.*/wsrep_commit/i;
      $pstate =~ s/^wsrep_aborter_idle.*/wsrep/i;
      $pstate =~ s/^(.+?);.*/$1/;
      $pstate =~ s/[^a-zA-Z0-9_]/_/g;

      if (exists $keys{'plist'}{$pstate}) {
        $states{$pstate}++;
      } else {
        plugin_log(LOG_WARNING, "MySQL: Unknown pstate: '$pstate'");
      }
    } else {
      $states{'null'}++;
    }
  }

  for (keys %{$keys{'status'}}) {
    my $vl = {};
    $vl->{'plugin'} = 'mysql';
    $vl->{'type'} = 'counter';
    $vl->{'plugin_instance'} = 'status';
    $vl->{'type_instance'} =  $_;
    if (defined($status->{$_}->{'Value'})) {
      if ($status->{$_}->{'Value'} =~ /^\d+(\.\d+)?$/) {
        $vl->{'values'} = [  $status->{$_}->{'Value'} + 0 ];
      } else {
        if ($status->{$_}->{'Value'} =~ /(?:yes|on|enabled)/i) {
          $vl->{'values'} = [ 1 ];
        } else {
          $vl->{'values'} = [ 0 ];
        }
      }
    } else {
      $vl->{'values'} = [ 0 ];
    }
    plugin_dispatch_values($vl);  
  }

  for (keys %{$keys{'slave'}}) {
    my $vl = {};
    $vl->{'plugin'} = 'mysql';
    $vl->{'type'} = 'counter';
    $vl->{'plugin_instance'} = 'slave';
    $vl->{'type_instance'} =  $_;
    if (defined($slave->{$_})) {
      if ($slave->{$_} =~ /^\d+(?:\.\d+)?$/) {
        $vl->{'values'} = [ $slave->{$_} + 0];
      } else {
        if ($slave->{$_} =~ /(?:yes|on|enabled)/i) {
          $vl->{'values'} = [ $slave->{$_} ];
        } else {
          $vl->{'values'} = [ 0 ];
        }
      }
    } else {
       $vl->{'values'} = [ 0 ];
    }
    plugin_dispatch_values($vl);  
  }

  my $vl = {};
  $vl->{'plugin'} = 'mysql';
  $vl->{'type'} = 'counter';
  $vl->{'plugin_instance'} = 'slave';
  $vl->{'type_instance'} =  'binlog_synched_to_master';

  if ($slave->{'Master_Log_File'} eq $slave->{'Relay_Master_Log_File'}) { ## Slave processing same binlog as master
    $vl->{'values'} = [ 1 ];
  } else {
    $vl->{'values'} = [ 0 ];
  }
  plugin_dispatch_values($vl);  

  foreach my $section (keys %{$keys{'innodb'}}) {
    my $vl = {};
    $vl->{'plugin'} = 'mysql';
    $vl->{'type'} = 'counter';
    $vl->{'plugin_instance'} = 'innodb';
    foreach my $item (keys %{$keys{'innodb'}{$section}}) {
      $vl->{'type_instance'} =  $section . '_' . $item;
      if ($innodb_status->{'sections'}->{$section}->{$item} =~ /^\d+(?:\.\d+)?$/) {
        $vl->{'values'} = [ $innodb_status->{'sections'}->{$section}->{$item} + 0];
      } else {
        if ($innodb_status->{'sections'}->{$section}->{$item} =~ /(?:yes|on|enabled)/i) {
          $vl->{'values'} = [ 1 ];
        } else {
           $vl->{'values'} = [ 0 ];
        }
      }
      plugin_dispatch_values($vl);  
    }
  }

  foreach my $item (keys %{$keys{'pstate'}}) {
    my $vl = {};
    $vl->{'plugin'} = 'mysql';
    $vl->{'type'} = 'counter';
    $vl->{'plugin_instance'} = 'process';
    $vl->{'type_instance'} =  $item;
    if (defined($states{$item})) {
      $vl->{'values'} = [ $states{$item} + 0];
    } else {
      $vl->{'values'} = [ 0 ];
    }
    plugin_dispatch_values($vl);  
  }

#  plugin_log(LOG_ERR, "MySQL: finished submitting values");
  return 1;
}


1;


package Collectd::Plugin::Riak;
use Collectd qw(:all);
use LWP::UserAgent;
use HTTP::Request::Common qw(GET);
use JSON;

=head1 NAME

Collectd::Plugins::Riak - Monitor a Riak node

=head1 VERSION

Version 1

=cut

our $VERSION = '1';


=head1 SYNOPSIS

This is a collectd plugin for monitoring a Riak node. It depends on following perl modules: LWP::UserAgent, HTTP::Request::Common, and JSON.

In your collectd config:

    <LoadPlugin "perl">
    	Globals true
    </LoadPlugin>

    <Plugin "perl">
      BaseName "Collectd::Plugins"
      LoadPlugin "Riak"

     <Plugin "Riak">
       Host "localhost"
       Port "8098"
      </Plugin>
    </Plugin>

=head1 AUTHOR

Mark Steele, C<< <mark at control-alt-del.org> >>
    
=cut

my $host = 'localhost';
my $port = 8098;

my @fields = qw(converge_delay_last converge_delay_max converge_delay_mean converge_delay_min coord_redirs_total cpu_avg1 cpu_avg15 cpu_avg5 cpu_nprocs executing_mappers gossip_received handoff_timeouts ignored_gossip_total mem_allocated 
memory_atom memory_atom_used memory_binary memory_code memory_ets memory_processes memory_processes_used memory_system memory_total mem_total node_get_fsm_objsize_100 node_get_fsm_objsize_95 node_get_fsm_objsize_99 node_get_fsm_objsize_mean 
node_get_fsm_objsize_median node_get_fsm_siblings_100 node_get_fsm_siblings_95 node_get_fsm_siblings_99 node_get_fsm_siblings_mean node_get_fsm_siblings_median node_get_fsm_time_100 node_get_fsm_time_95 node_get_fsm_time_99 
node_get_fsm_time_mean node_get_fsm_time_median node_gets node_gets_total node_put_fsm_time_100 node_put_fsm_time_95 node_put_fsm_time_99 node_put_fsm_time_mean node_put_fsm_time_median node_puts node_puts_total pbc_active pbc_connects 
pbc_connects_total postcommit_fail precommit_fail read_repairs read_repairs_total rebalance_delay_last rebalance_delay_max rebalance_delay_mean rebalance_delay_min rejected_handoffs riak_kv_vnodeq_max riak_kv_vnodeq_mean riak_kv_vnodeq_median 
riak_kv_vnodeq_min riak_kv_vnodeq_total riak_kv_vnodes_running riak_pipe_vnodeq_max riak_pipe_vnodeq_mean riak_pipe_vnodeq_median riak_pipe_vnodeq_min riak_pipe_vnodeq_total riak_pipe_vnodes_running ring_creation_size ring_num_partitions 
rings_reconciled rings_reconciled_total sys_global_heaps_size sys_process_count sys_thread_pool_size vnode_gets vnode_gets_total vnode_index_deletes vnode_index_deletes_postings vnode_index_deletes_postings_total vnode_index_deletes_total 
vnode_index_reads vnode_index_reads_total vnode_index_writes vnode_index_writes_postings vnode_index_writes_postings_total vnode_index_writes_total vnode_puts vnode_puts_total);

plugin_register (TYPE_READ, 'Riak', 'my_read');
plugin_register (TYPE_CONFIG, "Riak", "riak_config");

sub riak_config {
    my ($ci) = @_;
    foreach my $item (@{$ci->{'children'}}) {
        my $key = lc($item->{'key'});
        my $val = $item->{'values'}->[0];
        if ($key eq 'host' ) {
            $host = $val;
        } elsif ($key eq 'port' ) {
            $port = $val;
        }
    }
    return 1;
}

sub my_read
{
  eval {
    my $ua = LWP::UserAgent->new;
    $ua->timeout(5);
    my $req = GET "http://$host:$port/stats";
    $res = $ua->request($req);
  };
  if ($@) {
    plugin_log(LOG_ERR, "Riak: exception fetching document by http");
    return 1;
  }

  if ($res->code ne '200') {
    plugin_log(LOG_ERR, "Riak: non-200 response");
    return 1;
  }
  my $contents = $res->content();
  my $ref;
  eval {
    $ref = decode_json($contents);
  };
  if ($@) {
    plugin_log(LOG_ERR, "Riak: exception decoding response");
    return 1;
  }
  plugin_log(LOG_ERR, "Riak: decoded response");

  foreach my $field (@fields) {
    my $vl = {};
    $vl->{'plugin'} = 'riak';
    $vl->{'type'} = 'counter';
    $vl->{'type_instance'} = $field;
    $vl->{'values'} = [ $ref->{$field} ? $ref->{$field} : 0 ];
    plugin_dispatch_values($vl);
  }
  return 1;
}

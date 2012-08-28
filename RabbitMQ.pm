package Collectd::Plugin::RabbitMQ;
use Collectd qw(:all);
use LWP::UserAgent;
use HTTP::Request::Common qw(GET);
use JSON;

=head1 NAME

Collectd::Plugins::RabbitMQ - Monitor RabbitMQ queues and message rates

=head1 VERSION

Version 1

=cut

our $VERSION = '1';


=head1 SYNOPSIS

This is a collectd plugin for monitoring message rates and queues on a RabbitMQ broker. It uses the RabbitMQ management plugin and depends on following perl modules: LWP::UserAgent, HTTP::Request::Common, and JSON.

In your collectd config:

    <LoadPlugin "perl">
    	Globals true
    </LoadPlugin>

    <Plugin "perl">
      BaseName "Collectd::Plugins"
      LoadPlugin "RabbitMQ"

     <Plugin "RabbitMQ">
       Username "user"
       Password "pass"
       Realm "RabbitMQ Management"
       Host "localhost"
       Port "55672"
      </Plugin>
    </Plugin>

The 'Realm' value is annoyingly dependant on the version of RabbitMQ you're running. It corresponds to the authentication realm that LWP::UserAgent will send credentials to (based on HTTP headers sent by the broker). For RabbitMQ 2.8.1, the value is "RabbitMQ Management", for 2.5.1 it's "Management: Web UI". If neither of these work, sniff the HTTP traffic to find the basic authentication realm.

=head1 AUTHOR

Mark Steele, C<< <mark at control-alt-del.org> >>
    
=cut

my $username = 'user';
my $password = 'pass';
my $host = 'localhost';
my $port = 55672;
my $realm = '';

plugin_register (TYPE_READ, 'RabbitMQ', 'my_read');
plugin_register (TYPE_CONFIG, "RabbitMQ", "rabbit_config");

sub rabbit_config {
  plugin_log(LOG_ERR, "RabbitMQ: reading configuration");
    my ($ci) = @_;
    foreach my $item (@{$ci->{'children'}}) {
        my $key = lc($item->{'key'});
        my $val = $item->{'values'}->[0];
        if ($key eq 'host' ) {
            $host = $val;
        } elsif ($key eq 'username' ) {
            $username = $val;
        } elsif ($key eq 'password' ) {
            $password = $val;
        } elsif ($key eq 'port' ) {
            $port = $val;
        } elsif ($key eq 'realm' ) {
            $realm = $val;
        }
    }
  plugin_log(LOG_ERR, "RabbitMQ: reading configuration done");
    return 1;
}

sub my_read
{
  plugin_log(LOG_ERR, "RabbitMQ: starting http request");
  eval {
    my $ua = LWP::UserAgent->new;
    $ua->timeout(5);
    $ua->credentials("$host:$port",$realm,$username,$password);
    my $req = GET "http://$host:$port/api/queues";
    $res = $ua->request($req);
  };
  if ($@) {
    plugin_log(LOG_ERR, "RabbitMQ: exception fetching document by http");
    return 1;
  }

  plugin_log(LOG_ERR, "RabbitMQ: finished http request");
  if ($res->code ne '200') {
    plugin_log(LOG_ERR, "RabbitMQ: non-200 response");
    return 1;
  }
  plugin_log(LOG_ERR, "RabbitMQ: got 200 response");

  my $contents = $res->content();
  my $ref;
  eval {
    $ref = decode_json($contents);
  };
  if ($@) {
    plugin_log(LOG_ERR, "RabbitMQ: exception decoding response");
    return 1;
  }
  plugin_log(LOG_ERR, "RabbitMQ: decoded response");

  my $vl = {};
  $vl->{'plugin'} = 'rabbitmq';
  $vl->{'type'} = 'rabbitmq';

  foreach my $result (@{$ref}) {
    $vl->{'plugin_instance'} = $result->{'vhost'};
    $vl->{'type_instance'} = $result->{'name'};
    $vl->{'plugin_instance'} =~ s#[/-]#_#g;
    $vl->{'type_instance'} =~ s#[/-]#_#g;
    $vl->{'values'} = [ 
      $result->{'messages'} ? $result->{'messages'} : 0, 
      $result->{'messages_details'}->{'rate'} ? $result->{'messages_details'}->{'rate'} : 0,
      $result->{'messages_unacknowledged'} ? $result->{'messages_unacknowledged'} : 0, 
      $result->{'messages_unacknowledged_details'}->{'rate'} ? $result->{'messages_unacknowledged_details'}->{'rate'} : 0,
      $result->{'messages_ready'} ? $result->{'messages_ready'} : 0, 
      $result->{'message_ready_details'}->{'rate'} ? $result->{'message_ready_details'}->{'rate'} : 0,
      $result->{'memory'} ? $result->{'memory'} : 0, 
      $result->{'consumers'} ? $result->{'consumers'} : 0, 
      $result->{'message_stats'}->{'publish'} ? $result->{'message_stats'}->{'publish'} : 0,
      $result->{'message_stats'}->{'publish_details'}->{'rate'} ? $result->{'message_stats'}->{'publish_details'}->{'rate'} : 0,
      $result->{'message_stats'}->{'deliver_no_ack'} ? $result->{'message_stats'}->{'deliver_no_ack'} : 0,
      $result->{'message_stats'}->{'deliver_no_ack_details'}->{'rate'} ? $result->{'message_stats'}->{'deliver_no_ack_details'}->{'rate'} : 0,
      $result->{'message_stats'}->{'deliver_get'} ? $result->{'message_stats'}->{'deliver_get'} : 0,
      $result->{'message_stats'}->{'deliver_get_details'}->{'rate'} ? $result->{'message_stats'}->{'deliver_get_details'}->{'rate'} : 0,
    ];  
    plugin_log(LOG_ERR, "RabbitMQ: dispatching stats for " . $result->{'vhost'} . '/' . $result->{'name'});
    plugin_dispatch_values($vl);
  }
  plugin_log(LOG_ERR, "RabbitMQ: done processing results");
  return 1;
}

package Collectd::Plugins::AmqpJsonUdpUdp;

use strict;
use warnings;
use Collectd qw( :all );
use threads::shared;
use IO::Socket;
use JSON;

=head1 NAME

Collectd::Plugins::AmqpJsonUdp - Send collectd metrics to AMQP in json format, based on Collectd::Plugins::Graphite by Joe Miller

=head1 VERSION

Version 1

=cut

our $VERSION = '1';


=head1 SYNOPSIS

This is a collectd plugin for sending collectd metrics to AMQP in json format using the UDP exchange for RabbitMQ. This is fire and forget!

In your collectd config:

    <LoadPlugin "perl">
    	Globals true
    </LoadPlugin>

    <Plugin "perl">
      BaseName "Collectd::Plugins"
      LoadPlugin "AmqpJsonUdp"

    	<Plugin "AmqpJsonUdp">
    	  Buffer "65507"
    	  Prefix "datacenter"
    	  Host   "amqp.host"
    	  Port   "2003"
    	</Plugin>
    </Plugin>

=head1 AUTHOR

Mark Steele, C<< <mark at control-alt-del.org> >>, original author of graphite plugin Joe Miller
    
=cut

my $buff :shared;
my $buffer_size = 8192;
my $prefix;
my $host = 'localhost';
my $port = 5672;

sub amqp_json_config {
    my ($ci) = @_;
    foreach my $item (@{$ci->{'children'}}) {
        my $key = lc($item->{'key'});
        my $val = $item->{'values'}->[0];

        if ($key eq 'buffer' ) {
            $buffer_size = $val;
        } elsif ($key eq 'prefix' ) {
            $prefix = $val;
        } elsif ($key eq 'host') {
            $host = $val;
        } elsif ($key eq 'port') {
            $port = $val;
        }
    }

    return 1;
}

sub amqp_json_write {
    my ($type, $ds, $vl) = @_;
    my $host = $vl->{'host'};
    $host =~ s/\./_/g;
    my $hashtemplate = {};
    $hashtemplate->{'plugin'} = $vl->{'plugin'};
    $hashtemplate->{'type'}  = $vl->{'type'};
    if ( defined $vl->{'plugin_instance'} ) {
        $hashtemplate->{'plugin_instance'} = $vl->{'plugin_instance'};
    }
    if ( defined $vl->{'type_instance'} ) {
        $hashtemplate->{'type_instance'} = $vl->{'type_instance'};
    }

    my $bufflen;
    {
      lock($buff);
      for (my $i = 0; $i < scalar (@$ds); ++$i) {
          my $hashref = $hashtemplate;
          $hashref->{'name'} = $ds->[$i]->{'name'};
          $hashref->{'value'} = $vl->{'values'}->[$i];
          $hashref->{'time'} = $vl->{'time'};
          $hashref->{'datacenter'} = $prefix;
          $hashref->{'host'} = $host;
          $buff .= encode_json($hashref) . "\n";
      }
      $bufflen = length($buff);
    }
    if ( $bufflen >= $buffer_size ) {
        send_to_amqp();
    }
    return 1;
}

sub send_to_amqp {
     # Best effort to send
     lock($buff);
     return 0 if !length($buff);
     my $sock = IO::Socket::INET->new(Proto => 'udp',PeerPort => $port,PeerAddr => $host) or plugin_log(LOG_ERR, "AmqpJsonUdp.pm: Unable to connect to udp socket $host:$port");
     $sock->send($buff) or plugin_log(LOG_ERR, "AmqpJsonUdp.pm: Unable to send data");
     $buff = '';
     return 1;
}

sub amqp_json_flush {
    send_to_amqp();
    return 1;
}

plugin_register (TYPE_CONFIG, "AmqpJsonUdp", "amqp_json_config");
plugin_register (TYPE_WRITE, "AmqpJsonUdp", "amqp_json_write");
plugin_register (TYPE_FLUSH, "AmqpJsonUdp", "amqp_json_flush");

1; # End of Collectd::Plugins::AmqpJsonUdp

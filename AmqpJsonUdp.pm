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
        } elsif ($key eq 'user') {
            $user = $val;
        } elsif ($key eq 'password') {
            $password = $val;
        } elsif ($key eq 'exchange') {
            $exchange = $val;
        } elsif ($key eq 'vhost') {
            $vhost = $val;
        }
    }

    return 1;
}

sub amqp_json_write {
    my ($type, $ds, $vl) = @_;
    my $host = $vl->{'host'};
    $host =~ s/\./_/g;
    my $plugin_str = $vl->{'plugin'};
    my $type_str   = $vl->{'type'};   
    if ( defined $vl->{'plugin_instance'} ) {
        $plugin_str .=  "-" . $vl->{'plugin_instance'};
    }
    if ( defined $vl->{'type_instance'} ) {
        $type_str .= "-" . $vl->{'type_instance'};
    }

    my $bufflen;
    {
      lock($buff);
      for (my $i = 0; $i < scalar (@$ds); ++$i) {
          my $metric = sprintf "%s.%s.%s",
              $plugin_str,
              $type_str,
	      $ds->[$i]->{'name'};
          # convert any spaces that may have snuck in
          $metric =~ s/\s+/_/g;
          my $hashref = {};
          $hashref->{'type'} = 'metric';
          $hashref->{'metric'} = $metric;
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
     my $sock = IO::Socket::INET->new(Proto => 'udp',PeerPort => $port,PeerAddr => $host);
     $sock->send($buff);
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

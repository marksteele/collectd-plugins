package Collectd::Plugins::CloudWatchMetrics;

use strict;
use warnings;
use Collectd qw( :all );
use Paws;
use Paws::CloudWatch::MetricDatum;
use Paws::CloudWatch::Dimension;

=head1 NAME

Collectd::Plugins::CloudWatchMetrics - Sends metrics to CloudWatch

=head1 VERSION

Version 1

=cut

our $VERSION = '1';


=head1 SYNOPSIS

This is a collectd plugin for sending collectd metrics to CloudWatch

In your collectd config:

LoadPlugin perl
<Plugin perl>
  IncludeDir "/usr/lib64/collectd/perl"
  BaseName "Collectd::Plugins"
  LoadPlugin "CloudWatchMetrics"
  <Plugin CloudWatchMetrics>
    Region "us-east-1"
    AccessKey "AKIAI3243242IB776DVASDFASDF"
    SecretKey "87GuZX3242/VwaFme234RszZg/2UfuuKASDFASDFASDFASDF"
  </Plugin>
</Plugin>

=head1 AUTHOR

Mark Steele, C<< <mark at control-alt-del.org> >>
    
=cut

my $region = 'us-east-1';

sub cw_config {
    my ($ci) = @_;
    foreach my $item (@{$ci->{'children'}}) {
        my $key = lc($item->{'key'});
        my $val = $item->{'values'}->[0];
        if ($key eq 'region') {
            $region = $val;
        } elsif ($key eq 'accesskey') {
	    $ENV{'AWS_ACCESS_KEY'} = $val;
	} elsif ($key eq 'secretkey') {
	    $ENV{'AWS_SECRET_KEY'} = $val;
	}
    }
    return 1;
}

sub cw_write {
    my ($type, $ds, $vl) = @_;
    my $cw = Paws->service('CloudWatch',region => $region);
    my @items = ();
    my @dimensions = (new Paws::CloudWatch::Dimension(Name => 'host', Value => $vl->{'host'}));
    push(@dimensions, new Paws::CloudWatch::Dimension(Name => 'plugin',Value => $vl->{'plugin'}));
    push(@dimensions, new Paws::CloudWatch::Dimension(Name => 'type',Value => $vl->{'type'}));
    if ( defined $vl->{'plugin_instance'} ) {
	push(@dimensions, new Paws::CloudWatch::Dimension(Name => 'plugin_instance',Value => $vl->{'plugin_instance'}));
    }
    if ( defined $vl->{'type_instance'} ) {
	push(@dimensions, new Paws::CloudWatch::Dimension(Name => 'type_instance',Value => $vl->{'type_instance'}));
    }
    for (my $i = 0; $i < scalar (@$ds); ++$i) {
	push(@items, 
	     new Paws::CloudWatch::MetricDatum(
		 'MetricName' => $ds->[$i]->{'name'},
		 'Unit' => 'Count',
		 'Value' => $vl->{'values'}->[$i],
		 'Dimensions' => \@dimensions));
    }
    if (scalar(@items)) {
	my $cw = Paws->service('CloudWatch',region => $region);
	my $res = $cw->PutMetricData(MetricData => \@items, Namespace => 'Collectd');
	#plugin_log(LOG_ERR, "CW: Sent data: $res");
    } 
    return 1;
}

plugin_register (TYPE_CONFIG, "CloudWatchMetrics", "cw_config");
plugin_register (TYPE_WRITE, "CloudWatchMetrics", "cw_write");

1; # End of Collectd::Plugins::CloudWatchMetrics

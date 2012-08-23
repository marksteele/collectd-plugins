package Collectd::Plugin::HTTPCheck;
use Collectd qw(:all);
use LWP::UserAgent;
use HTTP::Request::Common qw(GET);
use JSON;

my $url;
my $regex;
my $instance = 'default';
my $check_type = 'plain';
my $check_field = 'default';
my $check_expected = 'default';

plugin_register (TYPE_READ, 'HTTPCheck', 'my_read');
plugin_register (TYPE_CONFIG, "HTTPCheck", "httpcheck_config");

sub httpcheck_config {
    plugin_log(LOG_ERR, "HTTPCheck: reading config values");
    my ($ci) = @_;
    foreach my $item (@{$ci->{'children'}}) {
        my $key = lc($item->{'key'});
        my $val = $item->{'values'}->[0];
        if ($key eq 'url' ) {
            $url = $val;
        } elsif ($key eq 'instance' ) {
            $instance = $val;
        } elsif ($key eq 'regex' ) {
            $regex = $val;
        } elsif ($key eq 'checktype' ) {
            $check_type = $val;
        } elsif ($key eq 'checkfield' ) {
            $check_field = $val;
        } elsif ($key eq 'checkexpected' ) {
            $check_expected = $val;
        }
    }
    plugin_log(LOG_ERR, "HTTPCheck: done reading configuration");
    return 1;
}

sub my_read
{
  my $vl = {};
  $vl->{'plugin'} = 'http';
  $vl->{'type'} = 'gauge';
  $vl->{'type_instance'} = $instance;
  $vl->{'values'} = [ 0 ];
  my $res;
  eval {
    my $ua = LWP::UserAgent->new;
    $ua->timeout(5);
    my $req = GET $url;
    $res = $ua->simple_request($req);
  };
  if ($@) {
    plugin_log(LOG_ERR, "HTTPCheck: caught exception");      
  }
#  plugin_log(LOG_ERR, "HTTPCheck: done fetching http document");

  if ($res->code ne '200') {
    plugin_log(LOG_ERR, "HTTPCheck: non 200");
    plugin_dispatch_values($vl);
    return 1;
  }

  my $contents = $res->content();
  if ($check_type eq 'default') {
    if ($contents =~ /$regex/) {
#      plugin_log(LOG_ERR, "HTTPCheck: Regex match");
      $vl->{'values'} = [ 1 ];
    } else {
      plugin_log(LOG_ERR, "HTTPCheck: Regex non-match: " . $contents);
    }
  } elsif ($check_type eq 'json') {
    eval {
      my $data = decode_json($contents);
      if ($data->{$check_field} eq $check_expected) {
#        plugin_log(LOG_ERR, "HTTPCheck: field match");
        $vl->{'values'} = [ 1 ];
      } else {
        plugin_log(LOG_ERR, "HTTPCheck: field not match: " . $data->{$check_field});
      }
    };
    if ($@) {
        plugin_log(LOG_ERR, "HTTPCheck: caught exception");      
    }
  }
  plugin_dispatch_values($vl);
  return 1;
}


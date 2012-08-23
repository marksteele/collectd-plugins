package Collectd::Plugin::CPUSummary;
use Collectd qw(:all);
use Sys::CPU;
plugin_register (TYPE_READ, 'cpusummary', 'my_read');

my $cpus = Sys::CPU::cpu_count();

sub my_read
{
        open(F,"/proc/stat");
        my $line = <F>;
        close(F);
        chomp($line);
        my $vl = {};
        $vl->{'plugin'} = 'cpusummary';
        $vl->{'type'} = 'cpusummary';
        $vl->{'values'} = [ (map { sprintf("%d",$_/$cpus); } (split(/\s+/,$line))[1..7]), $cpus ];
        plugin_dispatch_values($vl);
        return 1;
}


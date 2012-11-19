package Collectd::Plugin::CPUSummary;
use Collectd qw(:all);
use Sys::CPU;
#
# Add the following line to /usr/share/collectd/types.db:
# cpusummary              user:COUNTER:U:U,nice:COUNTER:U:U,system:COUNTER:U:U,idle:COUNTER:U:U,iowait:COUNTER:U:U,irq:COUNTER:U:U,softirq:COUNTER:U:U,cpucount:GAUGE:U:U
#



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

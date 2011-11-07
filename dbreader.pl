#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";
use DB_File;

$dbfile = $ARGV[0];
$pidfile = $ARGV[1];

if ($dbfile)
{
    tie %db, 'DB_File', $dbfile;
    if ($pidfile)
    {
	tie %PIDS, 'DB_File', $pidfile;
    }
    %pids = %PIDS;

    foreach $key (sort {$db{$a} <=> $db{$b}} keys %db)
    {
	my $tcn = $db{$key};
	
	unless ($pidfile)
	{
	    print "$key => $tcn\n";
	}
	else
	{
	    print "$key => $pids{$tcn}\n";
	}
    }
    untie %db;
    untie %PIDS if ($pidfile);
}
else
{
    print "Usage: ./dbreader.pl dbfilename [pidfile]\n";
}

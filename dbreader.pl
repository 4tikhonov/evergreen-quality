#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";
use DB_File;

$dbfile = $ARGV[0];

if ($dbfile)
{
    tie %db, 'DB_File', $dbfile;
    foreach $key (sort {$db{$a} <=> $db{$b}} keys %db)
    {
	print "$key => $db{$key}\n";
    }
    untie %db;
}
else
{
    print "Usage: ./dbreader.pl dbfilename\n";
}

#!/usr/bin/perl

use WWW::Mechanize;
use JSON -support_by_pp;

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/lib";

use lib '/usr/src/Evergreen-ILS-2.0.3/Open-ILS/src/perlmods';

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use OpenSRF::System;
use OpenILS::Utils::Fieldmapper;
use OpenSRF::Utils::SettingsClient;
use OpenSRF::EX qw/:try/;
use Encode;
use Unicode::Normalize;
use OpenILS::Application::AppUtils;
use OpenILS::Application::Cat::BibCommon;
use DBI;
use Quality;

use Getopt::Std;
%options=();
getopts("p:i:b:o:a:df:r",\%options);

$rec_id = $options{i} if ($options{i});
$barcode = $options{b} if ($options{b});
$org = $options{o} if ($options{o});
$authkey = $options{a} if ($options{a});
$filename = $options{f} if ($options{f});
$makerepair++ if ($options{r});
$DEBUG++ if ($options{d});

MARC::Charset->assume_unicode(1);
my %dbconfig = loadconfig("$Bin/config/quality.cfg");
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

my ($start_id, $end_id);
my $bootstrap = '/openils/conf/opensrf_core.xml';
$leaderfile = "$Bin/config/leaders.map";

OpenSRF::System->bootstrap_client(config_file => $bootstrap);
Fieldmapper->import(IDL => OpenSRF::Utils::SettingsClient->new->config_value("IDL"));

# must be loaded and initialized after the IDL is parsed
use OpenILS::Utils::CStoreEditor;
OpenILS::Utils::CStoreEditor::init();

my $e = OpenILS::Utils::CStoreEditor->new;

die "Error: can't find TCN or file...\n" if (!$rec_id && !$filename);
@record_list = getrecords($filename) if ($filename);
push(@record_list, $rec_id) if ($rec_id);

foreach $rec_id (@record_list)
{
    my $record = $e->retrieve_biblio_record_entry($rec_id);

    my ($status, $newxml, $marc) = is_wrong($record->marc, 1);
    my $action;

    $action = "change" if ($status=~/(repeat|unorder)/);
    $icount++;
    print "[$icount] $rec_id Changed $status\n" if ($status);

    if ($status=~/unordered/i)
    {
       $newxml = sorting_marc($newxml);
       print "Sorted\n$newxml\n";
    }

    if ($makerepair && $status=~/(repeat|lang)/)
    {
	my $editor = OpenILS::Utils::CStoreEditor->new(xact=>1);
        $newxml =~ s/\n//sgo;
	$newxml=~s/<record>/<record    xmlns\:xsi\=\"http\:\/\/www.w3.org\/2001\/XMLSchema-instance\" xsi\:schemaLocation\=\"http\:\/\/www\.loc\.gov\/MARC21\/slim http\:\/\/www\.loc\.gov\/standards\/marcxml\/schema\/MARC21slim\.xsd\"    xmlns\=\"http\:\/\/www\.loc\.gov\/MARC21\/slim\">/g;
        $newxml =~ s/^<\?xml.+\?\s*>//go;
        $newxml =~ s/>\s+</></go;
        $newxml =~ s/\p{Cc}//go;
        $newxml=~s/(<\/datafield>)/$1\n/g;
	$newxml=~s/(<\/controlfield>)/$1\n/g;

        my $xml = OpenILS::Application::AppUtils->entityize($newxml);

        $record->marc($xml);
	print "Update $rec_id\n";
        $editor->update_biblio_record_entry($record);
        $editor->commit();
    }

    if ($makerepair && $rec_id)
    {
	$icount++;
        $newxml =~ s/\n//sgo;
        $newxml=~s/<record>/<record    xmlns\:xsi\=\"http\:\/\/www.w3.org\/2001\/XMLSchema-instance\" xsi\:schemaLocation\=\"http\:\/\/www\.loc\.gov\/MARC21\/slim http\:\/\/www\.loc\.gov\/standards\/marcxml\/schema\/MARC21slim\.xsd\"    xmlns\=\"http\:\/\/www\.loc\.gov\/MARC21\/slim\">/g;
        $newxml =~ s/^<\?xml.+\?\s*>//go;
        $newxml =~ s/>\s+</></go;
        $newxml =~ s/\p{Cc}//go;
#        $newxml=~s/(<\/datafield>)/$1\n/g;
	$newxml.="</record>";

	my $quotexml = $dbh->quote($newxml);
	print "[$icount]\n";
	$dbh->do("update biblio.record_entry set marc=$quotexml where id='$rec_id'");
    }
};

sub openmarc
{
    my ($filename, $workmarc, $DEBUG) = @_;

    open(file, $filename);
    @marc = <file>;
    close(file);

    if ($workmarc)
    {
	push(@marc, $workmarc);
    }

    for ($i=0; $i<=$#marc; $i++)
    {
	$xml = $marc[$i];
	#$xml=~s/\r|\n//g;
	my ($status, $newxml, $marc) = is_wrong($xml);
	print "Changed\n" if ($status);
    }

    return %marc;
};

sub loadconfig
{
    my ($configfile, $DEBUG) = @_;
    my %config;

    open(conf, $configfile);
    while (<conf>)
    {
        my $str = $_;
        $str=~s/\r|\n//g;

        unless ($str=~/^\#/)
        {
            my ($name, $value) = split(/\s*\=\s*/, $str);
            $config{$name} = $value if ($value);
        };
    }
    close(conf);

    return %config;
}


sub getrecords
{
    my ($file, $DEBUG) = @_;

    open(file, $file);
    my @items = <file>;
    close(file);

    foreach $tcn (@items)
    {
	$tcn=~s/\r|\n//g;
	if ($tcn=~/^(\d+)\s+\S+/)
	{
	    $tcn = $1;
	}
	push(@records, $tcn) if ($tcn=~/^\d+$/); # && $tcn>915360);
    }

    return @records;
}

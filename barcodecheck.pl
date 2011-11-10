#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";

use Getopt::Std;
%options=();
getopts("db:h:l:o:p:",\%options);

$DEBUG++ if ($options{d});
$barcodefile = $options{b} if ($options{b});
$highresfile = $options{h} if ($options{h});
$lowresfile = $options{l} if ($options{l});
$outfile = $options{o} if ($option{o});
$pubdir = $options{p} if ($options{p});
$ORG = 10622;

$outdir = "$Bin/reports";
mkdir ($outdir) unless (-d $outdir);
$callfile = "$outdir/callnumbers.log";
loadcallnumbers($callfile);
loadfile('barcodes', $barcodefile);
die "Error: Can't find file with barcodes\n" unless (-e $barcodefile);
loadbarcodes('barcodeshigh', $highresfile);
die "Error: Can't find file with high resolution files\n" unless (-e $highresfile);
loadbarcodes('barcodeslow', $lowresfile);
die "Error: Can't find file with low resolution files\n" unless (-e $lowresfile);

$missedfile = "$outdir/missing.images.txt";
$nohighresfile = "$outdir/nohighres.images.txt";

open(missedfile, ">$missedfile");
open(nohigh, ">$nohighresfile");
foreach $barcode (sort keys %barcodes)
{
    $origbar = $barcode;
    $barcode=~s/$ORG\///g;

    my ($noimage, $nohighres) = (0, 0);

    # True: missing
    $noimage = 1 if (!$highres{$barcode} && !$lowres{$barcode});
    $nohighres = 1 if (!$highres{$barcode});

    if ($noimage)
    {
	$missing{$barcode} = $barcode;
	print missedfile "$callnumbers{$bar2id{$origbar}};;$barcode\n";
    }

    if ($nohighres)
    {
	print nohigh "$callnumbers{$bar2id{$origbar}};;$barcode\n";
    }
}
close(missedfile);
close(nohigh);

if (-d $pubdir)
{
   $pubdir=~s/\/$//g;
   my $cp1 = `cp -rf $missedfile $pubdir/`;
   my $cp2 = `cp -rf $nohighresfile $pubdir/`;
}

sub loadfile
{
    my ($type, $filename, $DEBUG) = @_;

    open(file, $filename);
    @content = <file>;
    close(file);

    foreach $str (@content)
    {
	$str=~s/\r|\n//g;
	my ($ID, $Advance, $barcode) = split(/\;\;/, $str);
	
	if ($ID=~/\d+/ && $barcode)
	{
	     $tcn{$ID} = $barcode;
	     $advance2tcn{$ID} = $Advance;
	     $barcodes{$barcode} = $barcode if ($barcode=~/\/3005/i);
	     $bar2adv{$barcode} = $Advance;
	     $bar2id{$barcode} = $ID;
	}
    }

    return;
}

sub loadbarcodes
{
    my ($type, $barfile, $DEBUG) = @_;

    # <barcode>30051000010436</barcode>
    open(barfile, $barfile);
    open(bartmp, ">$barfile.tmp");
    while (<barfile>)
    {
	$str = $_;
	$str=~s/(<barcode>|<\/barcode>)/\n/gsxi;	
	print bartmp "$str";
    }
    close(barfile);
    close(bartmp);

    open(barfile, "$barfile.tmp");
    while (<barfile>)
    {
        $str = $_;
	$str=~s/\r|\n//g;
	$barcode = $str;
	if ($str=~/\d+/)
	{
           $highres{$barcode}++ if ($type=~/high/i);
           $lowres{$barcode}++ if ($type=~/low/i);
	};
    }
    close(barfile);

    return;
}

sub loadcallnumbers
{
    my ($callfile, $DEBUG) = @_;

    open(file, $callfile);
    @callnum = <file>;
    close(file);

    foreach $str (@callnum)
    {
	$str=~s/\r|\n//g;
	my ($id, $callnum) = split(/\;\;/, $str);
	# BRO/1278/17/FOL
	$callnum_new = $callnum;
	$callnum_new=~s/\// /g;
	my @items = split(/\s+/, $callnum_new);

	my $callnum_p;
	foreach $item (@items)
	{
	    if ($item=~/^(\d+)/)
	    {
		$item = sprintf("%04d", $1);
	    }
	    if ($item=~/^(\w)(\d+)/)
	    {
		my ($i1, $i2) = ($1, $2);

		if ($i2 < 10)
		{
		    $item = sprintf("%s%03d", $i1, $i2);
		}
		elsif ($i2 >= 10 && $i2 < 100)
		{
		    $item = sprintf("%s%03d", $i1, $i2);
		}
		elsif ($i2 >= 100)
		{
		    $item = sprintf("%s%02d", $i1, $i2);
		}
	    }
	    $callnum_p.="$item ";
	}

	$callnum_p=~s/\s+$//g;
	$callnumbers{$id} = $callnum_p;
    }
    return;
}

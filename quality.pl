#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";
use lib "$libpath/lib";
use DB_File;
use Quality;

$dbpid = "/openils/applications/pids/pids.db";
$EXT_PIDS++ if (-e $dbpid);

if ($EXT_PIDS)
{
   tie %realtimepids, 'DB_File', $dbpid;
}

use Getopt::Std;
%options=();
getopts("dl:f:o:s:",\%options);

$DEBUG++ if ($options{d});
$intlimit = $options{l} if ($options{l});
$startid = $options{s} if ($options{s});
$finalid = $options{f} if ($options{f});
$intoffset = $options{o} if ($options{o});

my $out = 0;
my $LIMIT = 10000;
my $offset = $intoffset || 10000;
my $limit = $intlimit || $LIMIT;
my $finid = $finalid;
my $uid = 10622;

unless (keys %options)
{
print <<"EOL";
Evergreen Quality Checking System
(C) IISH 2011
Usage: quality.pl params
where params:
-s start ID in Evergreen 
-f final ID in Evergreen database
-l limit of records for one transaction
-o offset for each iteration

Example:
./quality.pl -s 1 -o 10000 -f 2000000
EOL
exit(0);
}

# Loading languages codes
%langcodes = loadlangcodes($Bin);

# Loading authority records order
%fields = loadconfig("$Bin/config/authority.cfg");
my @authority_fields = split(/\s+/, $fields{authfields});
foreach $tag (@authority_fields)
{
    $authority_fields{$tag}++;
}

# Loading problems list
%PROBLEMS = loadconfig("$Bin/config/problems.cfg");

use DBI;

my %config = loadconfig("$Bin/config/quality.cfg");
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($config{dbname}, $config{dbhost}, $config{dblogin}, $config{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

loadbarcodes($dbh);

# Reports
my ($AUTHORITY_LINKING_TEST, $SHORT_RECORDS_TEST, $NO_041_044_TEST, $MAKE_IMAGE_PID) = ($config{AUTHORITY_LINKING_TEST}, $config{SHORT_RECORDS_TEST}, $config{NO_041_044_TEST}, $config{MAKE_IMAGE_PID});
my ($ADVANCE2TCN, $MAKE_MARC, $CHECK_LEADER, $CHECK_REPEATED_FIELDS, $CHECK_ORDER_FIELDS) = ($config{ADVANCE2TCN}, $config{MAKE_MARC}, $config{CHECK_LEADER}, $config{CHECK_REPEATED_FIELDS}, $config{CHECK_ORDER_FIELDS});
my ($CHECK_COUNTRY_CODES) = ($config{CHECK_COUNTRY_CODES});
$useDB++ if ($ADVANCE2TCN);

if ($useDB)
{
   $dbdir = "$Bin/db";
   mkdir $dbdir unless (-e $dbdir);
   tie %advance, 'DB_File', "$dbdir/adv2id.db";
}

$logdir = $config{reportdir} || "$Bin/reports.tmp";
$mainlogdir=$logdir;
$mainlogdir=~s/\.tmp//g;
mkdir $logdir unless (-e $logdir);

open(advlog, ">$logdir/advance_holdings_missed.log");
open(slog, ">$logdir/holdings_missed.log");
open(linkedlog, ">$logdir/not_linked.log");
open(authlog, ">$logdir/notlinked.log") if ($AUTHORITY_LINKING_TEST);
open(idlog, ">$logdir/notlinked.id");
open(shortlog, ">$logdir/shorttitles.log") if ($SHORT_RECORDS_TEST);
open(pidslog, ">$logdir/pids.log");
open(langlog, ">$logdir/044a.log");
open(langwrong, ">$logdir/044a.wrong.log");
open(sortwrong, ">$logdir/sort.wrong.log");
open(advpids, ">$logdir/advpids.log");
open(marc, ">$MAKE_MARC") if ($MAKE_MARC);
open(repeated, ">$CHECK_REPEATED_FIELDS") if ($CHECK_REPEATED_FIELDS);
open(unorder, ">$CHECK_ORDER_FIELDS") if ($CHECK_ORDER_FIELDS);
open(callnumbers, ">$logdir/callnumbers.log");
checkall($startid, $finid);
close(advlog);
close(slog);
close(linkedlog);
close(authlog) if ($AUTHORITY_LINKING_TEST);
close(shortlog) if ($SHORT_RECORDS_TEST);
close(idlog);
foreach $advanceid (sort keys %advancepids) 
{
    print advpids "$advanceid;;http://hld.handle.net/$advancepids{$advanceid}\n";
}
close(advpids);
close(pids);
close(repeated);
close(unorder);

foreach $lang (sort {$wronglang{$b} <=> $wronglang{$a}} keys %wronglang)
{
    print langwrong "$lang $wronglang{$lang} TCN$tcnlang{$lang}\n";
}
close(langwrong);

foreach $lang (sort {$lang{$b} <=> $lang{$a}} keys %lang)
{
    print langlog "$lang $lang{$lang}\n";
}

if ($CHECK_LEADER)
{
    open(reportstat, ">$logdir/report.leader.log");
    open(subreport, ">$logdir/subtypes.log");
    foreach $type (sort {$report{$b} <=> $report{$a}} keys %report)
    {
	if ($leadercount)
	{
	    my $percent = sprintf("%.2f", ($report{$type} / $leadercount) * 100);
	    print reportstat "$type $report{$type} $percent%\n";
	};

	my %thissubtypes;
	%thissubtype = %{$subtypes{$type}} if ($subtypes{$type});
	foreach $utype (sort {$thissubtype{$b} <=> $thissubtype{$a}} keys %thissubtype)
	{
	    unless ($known{$utype})
	    {
	        my $percent = sprintf("%.2f", ($thissubtype{$utype} / $subcount) * 100);
	        print subreport "$utype $thissubtype{$utype} $percent% \n"; 
		$known{$utype}++;
	    };
	}
    }
    close(reportstat);
    close(subreport);
}
close(langlog);
close(sortwrong);
close(marc) if ($MAKE_MARC);
close(callnumbers);

if ($useDB)
{
    untie %advance;
}


if ($EXT_PIDS)
{
   untie %realtimepids;
};

if (-d $mainlogdir)
{
    `cp -rf $logdir/* $mainlogdir/`;
}

sub checkall
{
    my ($startid, $endid) = @_;
    my $true = 1;
    $startid = $offset if (!$startid && $offset);

    $currentid = $startid if ($startid);
    while ($true && $currentid < $endid)
    {
        $newid = $startid unless ($block);
        $newid = $startid + ($limit * $block) if ($block);
        $ending = $newid + $limit;

        print "[$block] Checking from $newid to $ending records... $endid\n";
	$lostid = getids($newid, $ending, $limit);
        $block++;
        $currentid = $ending;
	$lostids.="$lostid";
	$true = 0 if ($finid && $newid > $finid);
    }

    $xd = $newid + $limit;
    if ($xd <= $endid)
    {
	print "[$block] Checking from $newid for $limit records... $xd\n";
        print "$newid\n";
    }

    print "$lostids\n";
    
};

sub getMARC
{
    my ($ids, $idcount, $DEBUG) = @_;
    my ($reccount, %holdings, %noholdings, $warnid, $missed);

    $ids=~s/\,\s*$//g;
#    my $sqlquery = "select id, marc, create_date, editor, source from biblio.record_entry where id in ($ids)";
#    my $sqlquery = "select b.id, b.marc, b.create_date, b.editor, b.source, c.call_number, n.label_sortkey, c.barcode from asset.call_number as n, asset.copy as c, biblio.record_entry as b where n.id=c.call_number and n.record=b.id and b.id in ($ids)";

    # SQL
    my $sqlquery = "select b.id, b.marc, b.create_date, b.editor, b.source, c.call_number, n.label_sortkey, c.barcode from asset.call_number as n, asset.copy as c, biblio.record_entry as b where n.id=c.call_number and n.record=b.id and b.id in ($ids)";
    print "$sqlquery\n\n" if ($DEBUG2);
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($id, $marc, $date, $editor, $source, $callnumber, $sortkey, $barcode) = $sth->fetchrow_array())
    {
        $marc=~s/(<\/\S+?>)/$1\r\n/g;
        $marc=~s/\s+/ /g;
        $marc=~s/<datafield tag\=\"902\" ind1=\" \" ind2\=\" \">.+?<\/datafield>/g/;

        $sid = "eg";
        #$strpid = "oai:iish";
        $pid = "$uid/$sid/$id";
        $pid = "$strpid:$pid" if ($strpid);
        $pidstring= "\n<datafield tag=\"902\" ind1=\" \" ind2=\" \">\n  <subfield code=\"a\">$pid</subfield>\n</datafield>";

$locstring="
<datafield tag=\"852\" ind1=\"\" ind2=\"\">

    <subfield code=\"a\">IISG</subfield>
    <subfield code=\"b\">IISG</subfield>
    <subfield code=\"b\">IISG</subfield>
    <subfield code=\"c\">IISG</subfield>
    <subfield code=\"j\">$sortkey</subfield>
    <subfield code=\"p\">$barcode</subfield>

</datafield>";
        $pidstring.="$locstring\n" if (!$html && $out);

        $marc=~s/(<\/record>)/$pidstring $1/g;
        print "$marc\n" if ($out);
	$reccount++;
	$holdings{$id}++;
    }

#    unless ($idcount == $reccount)
    {
	foreach $id (sort keys %biblio)
	{
	    print "$id $holdings{$id}\n" if ($holdings{$id} > 1 && $DEBUG);
	    unless ($holdings{$id})
	    {
	        $warn_id++;
	        print "[Warning #$warn_id] No holding record for $id\n";
	 	print advlog "$id [$advance{$id}]\n";
		print slog "IISGb$advance{$id}\n";
		$missed.="$id [$advance{$id}], ";
	    };
	}
    }

    print "Holding records for $idcount: $reccount\n";
    return $missed;
}

sub getids
{
    my ($startid, $endid, $limit, $DEBUG) = @_;
    my ($ids, $idcount, $missed, $lost, %notlinked, %linked);

    my $sqlquery = "select b.id, b.marc, b.create_date, b.editor, b.source, c.call_number, n.label_sortkey, c.barcode from asset.call_number as n, asset.copy as c, biblio.record_entry as b where n.id=c.call_number and n.record=b.id";
    $sqlquery.=" order by random() limit $limit" if ($RANDOM);
    $sqlquery.=" and b.id >= $startid" if ($startid);
    $sqlquery.=" and b.id <= $endid" if ($endid);
    $sqlquery.=" order by b.id asc " if ($AUTHORITY_LINKING_TEST);
    #$sqlquery.=" order by b.id asc limit $limit" if ($offset);

    print "SQL $sqlquery\n"; # if ($DEBUG2);
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();
    %biblio = ();

    while (my ($id, $marc, $date, $editor, $source, $callnumber, $sortkey, $barcode) = $sth->fetchrow_array())
    {
	my $advanceid;

	if ($CHECK_REPEATED_FIELDS)
	{
	   my ($status, $newxml, $marc) = is_wrong($marc);
	   # Status: unordered, repeated, zero (it's ok)
	   print repeated "$id\n" if ($status=~/repeat/i);
	   print unorder "$id\n" if ($status eq 'unordered' && $CHECK_ORDER_FIELDS);
	};

	$marc=~s/\r|\n//g;
	print marc "$marc\n" if ($MAKE_MARC);

	if ($marc=~/IISG(\w+)/)
	{
	    my $advanceid = $1;
	    $advanceid=~s/\D+//g;
	    $advance{$advanceid} = $id;
	    $advancepids{$advanceid} = $realtimepids{$id};
	}

	if ($CHECK_LEADER)
	{
	    # extract content statistics
	    if ($marc=~/<leader>(.+?)<\/leader>/sxi)
	    {
		my $leader = $1;
		my ($subtype, $doctype);
		if ($marc=~/\"655\".+?code\=\"a\">(.+?)</)
                {
		    $subtype = $1;
		}
                if ($marc=~/\"245\".+?code\=\"k\">(.+?)</)
                {
                    $doctype = $1;
                }

		if ($leader=~/nam/i)
		{
		    $report{books}++;
		}
		elsif ($leader=~/nas/i)
		{
		    $report{serials}++;
		}
		elsif ($leader=~/nkm/i)
		{
		    $report{visual}++;
		    $subtypes{visual}{poster}++ if ($subtype=~/poster/i);
		    $subtypes{visual}{photo}++ if ($subtype=~/photo/i);
		    $subtypes{visual}{drawing}++ if ($subtype=~/drawing/i);
		    $subcount++ if ($subtype=~/(poster|photo|drawing)/i);
		}
		elsif ($leader=~/ngm/i)
		{
		    $report{movie}++;
		    $subtypes{movie}{video}++ if ($subtype=~/video/i);
		    $subtypes{movie}{dvd}++ if ($subtype=~/dvd/i);
		    $subcount++ if ($subtype=~/(video|dvd)/i);
		}
		elsif ($leader=~/nim/i)
		{
		    $report{sound}++;
		}
		elsif ($leader=~/nrm/i)
		{
		    $report{object}++;
		    $subtypes{object}{texttile}++ if ($subtype=~/texttile/i);
		    $subcount++ if ($subtype=~/texttile/i);
		}

		$leadercount++;
	    }

	}
        if ($callnumber)
        {
            $sortkey=~s/\_/\//g;
            $callnumbers{$id} = $sortkey;
        }

	if ($id > 0)
	{
	    print "$id\n" if ($out);
	    print pidslog "$id\n";
	    $ids.="$id,";
	    $idcount++;
	    $biblio{$id}++;

	    if ($marc=~/IISGb(\d+)/)
	    {
		$advanceid = $1;
		$advance{$id} = $advanceid;
	    }

	    # Images with barcodes
	    my ($handle, $trueimage);

	    if ($marc=~/hdl\.handle\.net\/(\S+?)</i)
	    {
		$handle = $1;
	    };

	    my $MAKE_IMAGE_PID;
	    if ($imagebars{$id})
	    {
	        $trueimage = $imagebars{$id};
		#$MAKE_IMAGE_PID = 1;
	    };
	    $trueimage = 0 if ($handle);

	    if ($trueimage)
	    {
		my $barcode = $trueimage || $handle;
		$old_barcodes{$advanceid} = $barcode;
		$barcodes{$id} = $barcode;
		$advancepids{$advanceid} = $barcode;

		if ($barcode && $MAKE_IMAGE_PID)
		{
		    #	id.realtime.pl -i 697211 -b 3005100111002
		    print "$id $advanceid $barcode\n";
		    $realtimepids{$id} = $barcode;
		    $barcode=~s/\d+\///g;
                    my $pidtmp;
		    if ($barcode=~/N/)
		    {
			$pidtmp = `/openils/applications/PID-webservice/examples/perl/pid.realtime.pl -i $id`;
	  	    }
		    else
		    {
			$pidtmp = `/openils/applications/PID-webservice/examples/perl/pid.realtime.pl -i $id -b $barcode`;
		    };
		    my $newpid;
                    if ($pidtmp=~/^(\d+)\s+\=>\s+(\S+)/)
                    {
			$newpid = $2;
			$realtimepids{$id} = $newpid;
		    }
		    print "Set to PID $newpid\n";
		}
	    };

	    # <datafield tag="044" ind1=" " ind2=" "><subfield code="a">ne</subfield></datafield>
	    if ($marc=~/\"044\".+?code\=\"a\">(.+?)</)
	    {
		my $lang = $1;
		$lang{$lang}++;

		unless ($langcodes{$lang})
		{
		    $tcnlang{$lang} = $id unless ($tcnlang{$lang});
		    $wronglang{$lang}++;
		}
	    }

	    if ($marc)
	    {
                @datatags = split(/<datafield\s+tag\=/i, $marc);
                foreach $tag (@datatags)
                {
                        print "[TAG $tag]\n" if ($DEBUG);
                        my ($tagid, $linktag);
                        if ($tag=~/^\"(\d+)\"/)
                        {
                            $tagid = $1;
                            $fields{$id}.="$tagid ";
                        }
		};
	    }

	    if ($SHORT_RECORDS_TEST)
	    {
		if ($marc=~/\"245\".+?code\=\"a\">(.+?)<\//)
		{
		    my $title = $1;
		    my $titlelen = length($title);
		    my @titlewords = split(/\s+/, $title);
		    my $wordslen = $#titlewords + 1;
		    print shortlog "TCN #$id *$titlelen*$wordslen* $title\n" if ($wordslen <= 2 && $SHORT_RECORDS_TEST);
		}
	    }

	    # SHORT_RECORDS_TEST
            if ($AUTHORITY_LINKING_TEST)
            {
                $linked = 0;

                if ($marc=~/<subfield code="0">(.+?)<\/subfield>/)
                {
                    $linked = $1;
                }
	    
		if ($linked)
	 	{
	            # Linking analysis
	            @datatags = split(/<datafield\s+tag\=/i, $marc);
	            foreach $tag (@datatags)
	            {
	 	        print "[TAG $tag]\n" if ($DEBUG);
			my ($tagid, $linktag);
			if ($tag=~/^\"(\d+)\"/)
			{
			    $tagid = $1;
			}
	
	                # <subfield code="0">(NL-AMISG)106278</subfield>
	                if ($tag=~/<subfield code="0">(.+?)<\/subfield>/)
	                {
		            $linktag = $1;
	                }

			if ($linktag)
			{
			    my $key = "tag$tagid";
	    	            print "\n$id $linked $marc\n" if ($finid && $linked && $DEBUG);
			    print "$id $tagid $linktag [$key]\n" if ($DEBUG);
			    $linked{$id}{$key} = $linktag;
			};
		    };
		};

		$notlinked{$id}++ unless ($linked{$id});
	    };
	};
    }

    foreach $id (sort keys %notlinked) # {$notlinked{$b} <=> $notlinked{$a}} keys %notlinked)
    {
#	print notlinked "$id\n";
	my @fields = split(/\s+/, $fields{$id});
	my ($problem, $status, $statusF) = (0, '', '');
	foreach $fieldid (@fields)
	{
	    if ($authority_fields{$fieldid})
	    {
	        $problem = $fieldid;
		$status.="$PROBLEMS{$fieldid}, " if ($PROBLEMS{$fieldid});
		$statusF{$id}.="$fieldid ";
	    };
	}

	if ($problem && !$known{$id})
	{
	    $notlinkedcount++;
	    $status=~s/\,\s*$//g;
	    print authlog "[$notlinkedcount] Not linked TCN #$id  	Fields: @fields\n" if ($AUTHORITY_LINKING_TEST);
	    print authlog "				Status: $status\n" if ($status && $AUTHORITY_LINKING_TEST);
	    print idlog "$id | $statusF{$id}\n";
	    $known{$id}++;
	};
    }

    if ($ids)
    {
	$missed = getMARC($ids, $idcount);
	$lost.="$missed";
    }
    print "\n";

    return $lost; 

}

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

sub loadlangcodes
{
    my ($Bin, $DEBUG) = @_;
    my %codes;

    open(codefile, "$Bin/codes.cfg");
    my @codes = <codefile>;
    close(codefile);

    foreach $code (@codes)
    {
        if ($code=~/^(\S+)/)
        {
            $codes{$1}++;
        }
    }

    return %codes;
}

sub loadbarcodes
{
    my ($dbh, $DEBUG) = @_;
    my (%barcodelist, $imagebars);

    $sqlquery = "select b.id, c.barcode from asset.call_number as n, asset.copy as c, biblio.record_entry as b where n.id=c.call_number and n.record=b.id ";
#    $sqlquery.=" limit 10";

    print "$sqlquery\n"; # if ($DEBUG2);
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    while (my ($id, $barcode) = $sth->fetchrow_array())
    {
	$barcodelist{$id} = $barcode;
	if ($barcode=~/^3005/)
	{
	    $imagebars{$id} = "$uid/$barcode";
	    $imagebars++;
	}
	if ($barcode=~/\/3005/)
	{
            $imagebars{$id} = "$uid/$barcode";
            $imagebars++;
	}
    };

    return %barcodelist;
}

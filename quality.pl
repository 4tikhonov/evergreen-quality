#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";

$logdir = "$Bin/logs";
mkdir $logdir unless (-e $logdir);

open(codefile, "$Bin/codes.txt");
@codes = <codefile>;
close(codefile);

foreach $code (@codes)
{
   if ($code=~/^(\S+)/)
   {
        $langcodes{$1}++;
   }
}

print "Content-type: text/html\n\n";
my $DEBUG = 0;
my $out = 0;
my $LIMIT = 10000;
my $offset = $ARGV[0] || 100;
my $limit = $ARGV[1] || $LIMIT;
my $finid = $ARGV[2];

my @authority_fields = (100, 110, 111, 600, 610, 611, 630, 648, 650, 651, 655, 700, 710, 711, 830);
foreach $tag (@authority_fields)
{
    $authority_fields{$tag}++;
}

%PROBLEMS = (	"100", "Personal Name field 100 isn't linked",
		"110", "Corporate Name field 110 isn't linked",
		"111", "Meeting Name field 111 isn't linked",
		"600", "Personal Name field 600 isn't linked",
		"610", "Corporate Name field 610 isn't linked",
		"611", "Meeting Name field 611 isn't linked",
		"630", "Uniform Title field 630 isn't linked",
		"648", "Chronological Term field 648 isn't linked",
		"650", "Topical Term field 650 isn't linked",
		"655", "Genre/Form field 655 isn't linked",
		"700", "Author field 700 is not linked",
		"710", "Additional author field 710 isn't linked",
		"711", "Organisation field 711 isn't linked",
		"830", "Uniform Title field 830 isn't linked"
);

# What to check 
$AUTHORITY_LINKING_TEST = 1;
$SHORT_RECORDS_TEST = 1;
$NO_041_044_TEST = 1;

use DBI;

my %dbconfig = loadconfig("$Bin/db.config");
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

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
checkall();
close(advlog);
close(slog);
close(linkedlog);
close(authlog) if ($AUTHORITY_LINKING_TEST);
close(shortlog) if ($SHORT_RECORDS_TEST);
close(idlog);
close(pids);

foreach $lang (sort {$wronglang{$b} <=> $wronglang{$a}} keys %wronglang)
{
    print langwrong "$lang $wronglang{$lang} TCN$tcnlang{$lang}\n";
}
close(langwrong);

foreach $lang (sort {$lang{$b} <=> $lang{$a}} keys %lang)
{
    print langlog "$lang $lang{$lang}\n";
}
close(langlog);
close(sortwrong);

sub checkall
{
    my ($startid, $endid) = (1, 1446220);
    my $true = 1;
    $startid = $offset if ($offset);

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

        $uid = 10622;
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

    $sqlquery = "select b.id, b.marc from biblio.record_entry as b where 1=1";
    $sqlquery.=" order by random() limit $limit" if ($RANDOM);
    $sqlquery.=" and b.id >= $startid" if ($startid);
    $sqlquery.=" and b.id <= $endid" if ($endid);
    $sqlquery.=" order by b.id asc " if ($AUTHORITY_LINKING_TEST);
    #$sqlquery.=" order by b.id asc limit $limit" if ($offset);

    print "$sqlquery\n"; # if ($DEBUG2);
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();
    %biblio = ();

    while (my ($id, $marc, $date, $editor, $source, $callnumber, $sortkey, $barcode) = $sth->fetchrow_array())
    {
	my $advanceid;

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
        my ($name, $value) = split(/\s*\=\s*/, $str);
        $config{$name} = $value;
    }
    close(conf);

    return %config;
}


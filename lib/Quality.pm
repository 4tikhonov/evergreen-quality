package Quality;

use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);

use Exporter;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;

$VERSION = 1.00;
@ISA = qw(Exporter);

@EXPORT = qw(
		fieldsexplorer
		analyze_order
		is_wrong
		sorting_marc
            );

sub fieldsexplorer
{

}

sub is_wrong
{
    my ($xml, $DEBUG) = @_;
    my ($marc, $changed, $tmpxml, $origxml, @ordertag, %marc, %repeat);

    if ($xml)
    {
        $xml =~ s/\n//sgo;
        $xml =~ s/^<\?xml.+\?\s*>//go;
#        $xml =~ s/<record.+?>/<record>/go;
        $xml =~ s/>\s+</></go;
        $xml =~ s/\p{Cc}//go;
        $xml=~s/(<\/datafield>)/$1\n/g;

        my @fields = split(/\n/, $xml);
        my (%known);
        foreach $field (@fields)
        {
	    my ($trusted, $tag) = (1, 0);
            if ($field=~/tag\=\"(\d+)\"/)
            {
                $tag = $1;
	    }
	    $trusted = 0 if ($known{$field});
	    $trusted = 0 if ($tag > 902);
	    $trusted = 0 unless ($tag);

	    if ($trusted)
	    {
                $origxml.="$field\n";
                unless ($known{$field})
                {
                   $tmpxml.="$field\n";
                }
                $known{$field}++;

                push(@ordertag, $tag);
                $marc{$field} = $tag;
                $repeat{$field}++;
	    };
        }

        $changed = 'repeated' if ($origxml ne $tmpxml);
        print "$tmpxml\n$changed\n$origxml\n" if ($DEBUG);
        #$marc = MARC::Record->new_from_xml($tmpxml);
    };

    my ($ordered, @problem) = analyze_order(@ordertag);
    print "[$ordered] @ordertag => Problem: @problem\n" if ($DEBUG);
    $changed = 'unordered' if (!$ordered && !$changed);

    return ($changed, $tmpxml, $marc);
}

sub sorting_marc
{
    my ($xml, $DEBUG) = @_;
    my ($xmlsorted, %marc);

    $xml=~s/(<\/datafield>)/$1\n/g;
    $xml=~s/(<\/controlfield>)/$1\n/g;
    $xml=~s/\n\n/\n/gsxi;

    my @lines = split(/\n/, $xml);
    foreach $line (@lines)
    {
        print "[DEBUG] $line\n" if ($DEBUG);
        if ($line=~/tag\=\"(\d+)\"/)
        {
            $marc{$line} = $1;
        }
    }

    # Generated sorted marc xml
    foreach $line (sort {$marc{$a} <=> $marc{$b}} keys %marc)
    {
        $xmlsorted.="$line\n";
    }

    print "$xmlsorted\n" if ($DEBUG);
    return $xmlsorted;
}

sub checking_marc
{
    my ($marc, $CLEAN, $DEBUG) = @_;
    my ($ORDER, @REPEAT, @ordertag, %repeat, $xmlsorted, %marc);

    my $xml = $marc->as_xml_record();
    if ($xml)
    {
            $xml =~ s/\n//sgo;
            $xml =~ s/^<\?xml.+\?\s*>//go;
            $xml =~ s/>\s+</></go;
            $xml =~ s/\p{Cc}//go;
            $xml=~s/(<\/datafield>)/$1\n/g;
    }

    my @lines = split(/\n/, $xml);
    foreach $line (@lines)
    {
        print "[DEBUG] $line\n" if ($DEBUG);
        if ($line=~/tag\=\"(\d+)\"/)
        {
            my $tag = $1;
            push(@ordertag, $tag);
            $marc{$line} = $1;
            $repeat{$line}++;
        }
    }

    # Generated sorted marc xml
    foreach $line (sort {$marc{$a} <=> $marc{$b}} keys %marc)
    {
        $xmlsorted.="$line\n";
    }

    print "$xmlsorted\n" if ($DEBUG);

    # Clean repeated fields
    foreach $line (sort {$repeat{$b} <=> $repeat{$a}} keys %repeat)
    {
        my $rcount = $repeat{$line};
        if ($rcount > 1 && $CLEAN)
        {
            for ($i=1; $i<=$rcount; $i++)
            {
                my $tag = $marc{$line};
                if (my $tagdel = $marc->field( $tag ))
                {
                   $marc->delete_field( $tagdel );
                }
                print "[$i] $repeat{$line} REPEAT $tag $line\n";
            }
        }
    }

    print $marc->as_xml_record();
    my ($ordered, @problem) = analyze_order(@ordertag);
    print "[$ordered] @ordertag => Problem: @problem\n";

    return $xmlsorted;

}

sub analyze_order
{
    my (@tags, $DEBUG) = @_;
    my (@problemtags, %key);
    my $ordered = 1;

    @sorttags = sort {$a <=> $b} @tags;
    print "@sorttags\n" if ($DEBUG);

    return $ordered if ("@sorttags" eq "@tags");

    for ($i=1; $i<=$#sorttags; $i++)
    {
        my ($tag1, $tag2) = ($sorttags[$i-1], $sorttags[$i]);
        $key{$tag2} = $tag1;
    }

    for ($i=1; $i<=$#tags; $i++)
    {
        my ($tag1, $tag2) = ($tags[$i-1], $tags[$i]);

        if ($tag1 > $tag2)
        {
            $ordered = 0;
            print "P $tag1 $tag2 => $key{$tag2}\n" if ($DEBUG);
            push(@problemtags, $tag2);
        }
    }

    return ($ordered, @problemtags);
}


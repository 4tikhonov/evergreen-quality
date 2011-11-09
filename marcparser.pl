#!/usr/bin/perl

my $level1 = 1;
my $level2 = 1;
my $level3 = 1;
my $level4 = 1;
my $level5 = 1;
my $level6 = 1;
my %codes = ("100|a", 1, "100|a", 2);
my $DEBUG = 0;
my $structure = "advanceID;;100|a;;111|a;;500|a;;541|a;;600|a;;600|b;;600|c;;610|a;;610|b;;611|a;;611|c;;611|d;;611|e;;611|n;;630|a;;630|x;;648|a;;650|a;;651|a;;655|a;;694|a;;694|b;;695|a;;695|c;;695|d;;695|e;;695|f;;695|g;;695|h;;695|w;;695|z;;700|a;;700|c;;700|e;;710|a;;710|b;;710|e;;711|a;;711|c;;711|d;;711|e;;711|n;;740|a";
print "$structure\n";

while (<>)
{
   my $str = $_;
   $str=~s/\r|\n//g;

   my @tags = split(/tag\=\"/, $str);

   my %reports;
   my ($s600, %s600);
   foreach $tag (@tags)
   {
	if ($tag=~/^(\d+)/)
	{
	    my $field = $1;
	    my @subfields = split(/subfield.+?code\=\"/, $tag);
	    foreach $subfield (@subfields)
	    {
		if ($subfield=~/^(\w+)\">(.+?)</)
		{
		    my ($subfield_new, $value) = ($1, $2);
#		    print "$field|$subfield_new => $value\n" unless ($field eq $subfield_new);
		    $reports{"$field|$subfield_new"} = $value;
		    if ($field=~/^(6|7)\d+/)
		    {
			$s600++;
			$s600{"$field|$subfield_new"} = $value;
	   	    }
		}
	    }
	}
   }

   my (%resultset, $advance);
   # <controlfield tag="001">IISGb10534445</controlfield>
   if ($str=~/tag="001">(\S+?)</)
   {
	$advance = $1;
	$advance=~s/\D+//g;
   }
   print "$advance;;" if ($advance);

   if ($level1 && $reports{"100|a"})
   {
	print $reports{"100|a"}."\n" if ($DEBUG);
	$resultset{"100|a"} = $reports{"100|a"};
	unless ($control{"100|a"})
	{
	   $idcount++;
	   $control{"100|a"} = $idcount; 
	};
   }

   if ($level2 && $reports{"110|a"} && $reports{"110|b"})
   {
	$resultset{"110|a"} = $reports{"110|a"};
	$resultset{"110|b"} = $reports{"110|b"};

	print $reports{"110|a"}.';;'.'$reports{"110|b"}'."\n" if ($DEBUG);
	unless ($control{"110|a"})
	{
	    $idcount++;
	    $control{"110|a"} = $idcount;
	};
   }

   if ($level2 && $reports{"111|a"})
   {
	$resultset{"111|a"} = $reports{"111|a"};

	unless ($control{"111|a"})
	{
	   $idcount++;
	   $control{"111|a"} = $idcount;
	};
        print $reports{"111|a"}."\n" if ($DEBUG);
   }

   if ($level3 && $reports{"260|a"} && $reports{"260|b"} && $reports{"260|c"})
   {
        $resultset{"260|a"} = $reports{"260|a"};
        $resultset{"260|b"} = $reports{"260|b"};
	$resultset{"260|c"} = $reports{"260|c"};

	unless ($control{"260|a"})
	{
        $idcount++;
        $control{"260|a"} = $idcount;
	};
	
	unless ($control{"260|b"})
	{
        $idcount++;
        $control{"260|b"} = $idcount;
	};

	unless ($control{"260|c"})
	{
        $idcount++;
        $control{"260|c"} = $idcount;
	};

        print $reports{"260|a"}.';;'.$reports{"260|b"}.';;'.$reports{"260|c"}."\n" if ($DEBUG);
   }

   if ($level4 && $reports{"500|a"})
   {
	$resultset{"500|a"} = $reports{"500|a"};

	unless ($control{"500|a"})
	{
           $idcount++;
           $control{"500|a"} = $idcount;
	};

        print $reports{"500|a"}."\n" if ($DEBUG);
   }

   if ($level5 && $reports{"541|a"})
   {
	$resultset{"541|a"} = $reports{"541|a"};

	unless ($control{"541|a"})
	{
        $idcount++;
        $control{"541|a"} = $idcount;
	};

        print $reports{"541|a"}."\n" if ($DEBUG);
   }

   if ($level6 && $s600)
   {
	foreach $tag (sort keys %s600)
	{
	    unless ($control{$tag})
	    {
		$idcount++;
		$control{$tag} = $idcount;
	    }

	    $resultset{"$tag"} = $s600{$tag};
	    print "$advance;;$tag;;$s600{$tag}\n" if ($DEBUG);
	};
   }

   if (keys %resultset)
   {
   my @structure = split(/\;\;/, $structure);
   foreach $field (@structure)
   {
	print "$resultset{$field};;";
   }
   print "\n";
   };


}

print "\n";
#foreach $tag (sort {$control{$a} <=> $control{$b}} keys %control)
foreach $tag (sort keys %control)
{
    print "$tag;;";
}

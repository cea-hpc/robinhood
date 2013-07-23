#!/usr/bin/env perl
#
# Process a definition file and generate a header with:
# - entry_info_t
# - field_infos array
# - several defines (attribute index, attribute mask, ...)
#
#
# usage: type_gen.pl <inputfile> <outputfile>
#

use strict;

if ( $#ARGV != 1 )
{
   print STDERR "Usage: type_gen.pl <inputfile> <outputfile>\n";
   exit -1;
}

my $infile = $ARGV[0];
my $outfile = $ARGV[1];

open( INPUT, "< $infile" ) or die "Could not open $infile";
open( OUTPUT, "> $outfile" ) or die "Could not open $outfile";

# 0: not yet initialized (only empty lines or comments expected)
# 1: header section
# 2: attr definition section
my $status=0;
my $line;
my $lineno=0;

my $next_index=0;
my %attrlist=();

print OUTPUT "#ifndef _APP_TYPES_H\n";
print OUTPUT "#define _APP_TYPES_H\n";

print OUTPUT "\n#include <stddef.h>\n";

while ( $line = <INPUT> )
{
	$lineno++;

	if ($line =~ m/^\s*\%header\s*$/ )
	{
	  # beginning of a new section
	  $status = 1;
	}
	elsif ($line =~ m/^\s*\%attrdef\s*$/ )
	{
	  # beginning of a new section
	  $status = 2;
	}
	elsif ( $status == 0 )
	{
		if ( ($line !~ m/^\s*$/) && ($line !~ m/^\s*#/) )
		{
			print STDERR "Unexepected line out of \%header or \%attrdef section: line $lineno: $line\n";
			exit -1;
		}
	}
	elsif ( $status == 1 )
	{
		print OUTPUT $line;
	}
	elsif ( $status == 2 )
	{
		# is this a comment or emptyline?
		next if ( ($line =~ m/^\s*$/) || ($line =~ m/^\s*#/) );

		my ($name,$ctype,$dbtype,$len,$flags,$gen_from,$gen_func);

		# parse the line
		if ( $line =~ m/^\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+)$/ )
		{
			($name,$ctype,$dbtype,$len,$flags,$gen_from,$gen_func) = ($1,$2,$3,$4,$5,"","NULL");
		}
		elsif (  $line =~ m/^\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*(.*)$/ )
		{
			($name,$ctype,$dbtype,$len,$flags,$gen_from,$gen_func) = ($1,$2,$3,$4,$5,$6,$7);
		}
		else
		{
			print STDERR "Bad format for attr definition: line $lineno: $line\n";
                	exit -1;
		}

		# remove trailing blanks
		$name =~ s/\s*$//;
		$ctype =~ s/\s*$//;
		$dbtype =~ s/\s*$//;
		$len =~ s/\s*$//;
		$flags =~ s/\s*$//;
		$gen_from =~ s/\s*$//;
		$gen_func =~ s/\s*$//;

		if ( $gen_from eq "" )
		{
			$gen_from = "-1";
		}
		else
		{
			$gen_from = "ATTR_INDEX_".$gen_from ;
		}

		${$attrlist{$next_index}}{name}=$name;
		${$attrlist{$next_index}}{ctype}=$ctype;
		${$attrlist{$next_index}}{dbtype}=$dbtype;
		${$attrlist{$next_index}}{len}=$len;
		${$attrlist{$next_index}}{flags}=$flags;
		${$attrlist{$next_index}}{gen_from}=$gen_from;
		${$attrlist{$next_index}}{gen_func}=$gen_func;

		$next_index ++;

	}
}

# printing data structures
my $index;

print OUTPUT "\ntypedef struct __entry_info__ \n{\n";

foreach $index (sort {0+$a <=> 0+$b}  keys %attrlist )
{
	print OUTPUT "\t". ${$attrlist{$index}}{ctype} . " ";
	print OUTPUT "\t". ${$attrlist{$index}}{name};

	# is len a number or a constant?
	if (${$attrlist{$index}}{len} !~ m/^[0-9]+/ )
	{
		print OUTPUT "[".${$attrlist{$index}}{len}."];\n";
	}
	elsif ( ${$attrlist{$index}}{len} > 0 )
	{
		print OUTPUT "[".${$attrlist{$index}}{len}."];\n";
	}
	else
	{
		print  OUTPUT ";\n";
	}
}

print OUTPUT "} entry_info_t;\n\n";

#print all defines (index and mask)

foreach $index (sort {0+$a <=> 0+$b}  keys %attrlist )
{
	print OUTPUT "#define ATTR_INDEX_".${$attrlist{$index}}{name}." \t$index\n";
}
print OUTPUT "\n";
print OUTPUT "#define ATTR_COUNT ".$next_index."\n";
print OUTPUT "\n";
foreach $index (sort {0+$a <=> 0+$b}  keys %attrlist )
{
        my $mask_val = 1<<$index;
	printf OUTPUT "#define ATTR_MASK_".${$attrlist{$index}}{name}." \t%#08X\n", $mask_val;
}

print OUTPUT "\nstatic const field_info_t field_infos[]=\n{\n";
foreach $index (sort {0+$a <=> 0+$b}  keys %attrlist )
{
	my $lenprint="";
	# is len a number or a constant?
	if (${$attrlist{$index}}{len} !~ m/^[0-9]+/ )
	{
		$lenprint = ${$attrlist{$index}}{len}."-1";
	}
	else
	{
		$lenprint = ${$attrlist{$index}}{len}-1;
	}

	if ( $index != $next_index - 1 )
	{
		print OUTPUT "\t{ \"".${$attrlist{$index}}{name}."\", \t".
			     ${$attrlist{$index}}{dbtype}.", \t".
			     $lenprint.", \t".
			     ${$attrlist{$index}}{flags}.
			     ", offsetof(entry_info_t, ".
			     ${$attrlist{$index}}{name}.") , ".
			     ${$attrlist{$index}}{gen_from}.", ".
			     ${$attrlist{$index}}{gen_func}." }, \n";
	}
	else
	{
		print OUTPUT "\t{ \"".${$attrlist{$index}}{name}."\", \t".
				${$attrlist{$index}}{dbtype}.", \t".
				$lenprint.", \t".
				${$attrlist{$index}}{flags}.
				", offsetof(entry_info_t, ".
				${$attrlist{$index}}{name}."), ".
				${$attrlist{$index}}{gen_from}.", ".
				${$attrlist{$index}}{gen_func}." }\n";
	}

}

print OUTPUT "};\n";
print OUTPUT "\n#endif\n";

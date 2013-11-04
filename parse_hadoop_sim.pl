use strict;

sub ParseRes {
	my ($fname) = @_;
	open IN, "<$fname" or die "$!\n";
	while (my $line = <IN>) {
		chomp $line;
		next unless $line=~/JOBID/;
		$line =~ /SUBMIT_TIME=\"(\d+)\" START_TIME=\"(\d+)\" FINISH_TIME=\"(\d+)\"/; 
		#print "$1, $2, $3\n";
		print $3-$1, "\n";
	}
	close IN;
	return 0;
}

# number of maps
# average map length
# average approximate map length
# number of reduces
# average reduce length
# average approximate reduce length <-- this is missing
# submission time
# approximation level
#216     140     60      1       15      5       0
sub GetLength {
	my ($base, $increment) = @_;
	return $base+int(rand()*$increment);
}

sub GenSynthetic {
	my ($njobs, $nint) = @_;
	my $timestamp =0;
	for (1 .. $njobs) {
		my $rnum = rand();
		my $nmaps = 0;
		my $increment = 20;
		$timestamp += int(rand()*$nint);
		if ($rnum < 0.05) {
			$nmaps = GetLength(70, $increment);
		} elsif ($rnum < 0.07) {
			$nmaps = GetLength(50, $increment);
		} elsif ($rnum < 0.11) {
			$nmaps = GetLength(30, $increment);
		} elsif ($rnum < 0.15) {
			$nmaps = GetLength(10, $increment);
		} else {
			$nmaps = int(rand()*10)+1;
		}
		print $nmaps, "\t140\t60\t1\t15\t$timestamp\t0\n"
	}
	return 0;
}


sub GenNmap {
	my ($nMaps, $ShuffleR) = @_;
	my $base=140;
	return int($base*(1+$ShuffleR));
}

sub GenNReduce {
	my ($nRed, $outR) = @_;
	my $base=15;
	my $randSeek=10;
	if ($outR/$nRed<1) {
		return int($base*(1+$outR));
	} else {
		return $base+int(log($outR/$nRed))*$randSeek;
	}
}

##SWIM format: my ($nSplit, $nReducer, $shuffleR, $outR, $thinkT)
sub GenSWIMTrace {
	my ($fname, $fout, $startPos, $nNum) = @_;
	my @lines=();
	my @output=();
	open IN, "<$fname" or die "$!\n";
	@lines=<IN>;
	chomp @lines;	
	close IN;
	my $clock = 0;
	for my $id($startPos .. $startPos+$nNum-1) {
		my ($nSplit, $nReducer, $shuffleR, $outR, $thinkT) = split /\s+/, $lines[$id];
		#print $lines[$id], "\n";
		my $lmap = GenNmap($nSplit, $shuffleR);
		my $lmapApprox = int($lmap*0.4);
		my $lred = GenNReduce($nReducer, $outR);
		## not used currently
		my $lredApprox = int($lred*0.4);
		my $str = $nSplit. "\t". $lmap."\t". $lmapApprox. "\t". $nReducer. "\t". $lred. "\t". $clock. "\t0";
		push @output, $str;
		$clock+=$thinkT;
	}
	open OUT, ">$fout" or die "$!\n";
	foreach my $e (@output) {
		print OUT $e, "\n";
	}
	close OUT;
	return 0; 
}

#GenSynthetic(@ARGV);
#GenSWIMTrace(@ARGV);
ParseRes(@ARGV);



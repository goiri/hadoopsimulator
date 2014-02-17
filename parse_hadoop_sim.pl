use strict;
use warnings;
use File::Basename;
sub ParseRes {
	my ($fname, $foutDir) = @_;
	my($base, $path, $ext) = fileparse($fname);
	my %jobHash=();
	my %mapHash=();
	my %reduceHash=();
	my %mapQHash=();
	my %mapEndTimeHash=();
	my %reduceQHash=();
	my $jobID;
	my @qtime;
	open IN, "<$fname" or die "$!\n";
	while (my $line = <IN>) {
		chomp $line;
		my $mykey=();
		my @val=();
		if ($line=~/JOBID/) {
			$line =~ /JOBID=\"(.*)\" JOB_STATUS=\"(.*)\" SUBMIT_TIME=\"(\d+)\" START_TIME=\"(\d+)\" FINISH_TIME=\"(\d+)\"/; 
			@val=($1, $2, $3, $4, $5);
			$mykey=$1;
			$mykey =~ s/job_//g;
			$jobHash{$mykey}=\@val;
			#print "$1, $2, $3\n";
		}
		if ($line=~/MapAttempt/) {
			$line =~ /TASKID=\"(.*)\" TASK_ATTEMPT_ID/; 
			$mykey=$1;
			$line =~ / START_TIME=\"(\d+)\" FINISH_TIME=\"(\d+)\"/; 
			@val=($mykey, $1, $2);
			$mykey =~ s/task_//g;
			$mapHash{$mykey}=\@val;
			if ($mykey=~/(.*)_m_/) {
				$jobID=$1;
				if (!defined $mapEndTimeHash{$jobID}) {
					$mapEndTimeHash{$jobID}=$val[2];
				} elsif ($mapEndTimeHash{$jobID}<$val[2]) {
					$mapEndTimeHash{$jobID}=$val[2];
				}
			}
			#print "$1, $2, $3\n";
		}
		if ($line=~/ReduceAttempt/) {
			$line =~ /TASKID=\"(.*)\" TASK_ATTEMPT_ID/; 
			$mykey=$1;
			$line =~ / START_TIME=\"(\d+)\" FINISH_TIME=\"(\d+)\"/; 
			@val=($mykey, $1, $2);
			$mykey =~ s/task_//g;
			$reduceHash{$mykey}=\@val;
			#print "$1, $2, $3\n";
		}

	}
	close IN;
	#job
	open OUT, ">$foutDir/$base.dat";
	for my $k(sort keys %jobHash) {
		print OUT $jobHash{$k}->[4]-$jobHash{$k}->[2], "\n";
	}
	close OUT;

	open OUT, ">$foutDir/$base.job";
	for my $k(sort keys %jobHash) {
		my $valRef = $jobHash{$k};
		my $concat = join ":", @$valRef;
		print OUT $k, "=", $concat, "\n";
	}
	close OUT;
	#map
	open OUT, ">$foutDir/$base.mtime";
	open OUT1, ">$foutDir/$base.mqtime";
	for my $k(sort keys %mapHash) {
		my $valRef = $mapHash{$k};
		print OUT $valRef->[2] - $valRef->[1], "\n";
		if ($k=~/(.*)_m_/) {
			$jobID=$1;
			#submit time, start time
			@qtime=($jobHash{$jobID}->[2], $valRef->[1]);
			print OUT1 $valRef->[1]-$jobHash{$jobID}->[2], "\n";
			$mapQHash{$k}=\@qtime;
		}
	}
	close OUT1;
	close OUT;

	open OUT, ">$foutDir/$base.mtask";
	for my $k(sort keys %mapHash) {
		my $valRef = $mapHash{$k};
		my $concat = join ":", @$valRef;
		print OUT $k, "=M=", $concat, "\n";
		my $valRef1 = $mapQHash{$k};
		my $concat1 = join ":", @$valRef1;
		print OUT $k, "=MQ=", $concat1, "\n";
	}
	close OUT;
	#reduce
	open OUT, ">$foutDir/$base.rtime";
	open OUT1, ">$foutDir/$base.rqtime";
	for my $k(sort keys %reduceHash) {
		my $valRef = $reduceHash{$k};
		print OUT $valRef->[2] - $valRef->[1], "\n";
		if ($k=~/(.*)_r_/) {
			$jobID=$1;
			#submit time, start time
			@qtime=($mapEndTimeHash{$jobID}, $valRef->[1]);
			print OUT1 $valRef->[1]-$mapEndTimeHash{$jobID}, "\n";
			$reduceQHash{$k}=\@qtime;
		}
	}
	close OUT1;
	close OUT;

	open OUT, ">$foutDir/$base.rtask";
	for my $k(sort keys %reduceHash) {
		my $valRef = $reduceHash{$k};
		my $concat = join ":", @$valRef;
		print OUT $k, "=R=", $concat, "\n";
		my $valRef2 = $reduceQHash{$k};
		my $concat2 = join ":", @$valRef2;
		print OUT $k, "=RQ=", $concat2, "\n";
	}
	close OUT;

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



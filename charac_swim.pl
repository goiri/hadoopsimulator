use strict;
use warnings;

# number of maps
# average map length
# average approximate map length
# number of reduces
# average reduce length
# average approximate reduce length <-- this is missing
# submission time
# approximation level

#input [0-2000], [2001, 979993123369]

sub Main {
	my ($fname, $interval) = @_;
	my $currTime=1;
	my $totalInputSize=0;
	my $scaleFactor=100;
	my $nMapsMin=2;
	my $nReduceMin=1;
	open IN, "<$fname" or die "$!\n";
	while (my $line=<IN>) {
		#print $line;
		chomp $line;
		my ($jobID, $timeStamp, $sleepTime, $inputSize, $shuffleSize, $outputSize) = split /\s+/, $line;
		my $nMaps=0;
		my $nMapLength=70;
		my $nReduces=0;
		my $nReduceLength=15;
		my $lfMapFactor=1;
		my $lfRedFactor=0.5;
		if ($inputSize<=2000) {
			$nMaps=$nMapsMin;
		} else {
			$nMaps=$nMapsMin+int(log($inputSize)*$lfMapFactor);
		}
		if ($inputSize+$outputSize<=2000) {
			$nReduces=$nReduceMin;
		} else {
			$nReduces=$nReduceMin+int(log($inputSize+$outputSize)*$lfRedFactor);
		}
		if ($shuffleSize>0 && log($shuffleSize) > $nMapLength) {
			$nMapLength += int(log($shuffleSize));
		}
		if ($inputSize+$outputSize>0 && log($inputSize+$outputSize) > $nReduceLength) {
			$nReduceLength += int(log($inputSize+$outputSize));
		}
		print $nMaps, "\t", $nMapLength, "\t", int(0.4*$nMapLength), "\t", $nReduces, "\t", $nReduceLength, "\t", $timeStamp, "\t", 0, "\n";
		if (int($timeStamp/$interval) > $currTime) {
			$currTime = int($timeStamp/$interval); 
			#print $inputSize, "\n";
		}
		#$totalInputSize+=$inputSize/1024/1024/1024;
	}
	close IN;
	return 0;
}

Main(@ARGV);


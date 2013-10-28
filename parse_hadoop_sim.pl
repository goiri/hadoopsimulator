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

sub Main {
	my ($njobs, $nint) = @_;
	for (1 .. $njobs) {
		my $rnum = rand();
		my $nmaps = 0;
		my $increment = 20;
		my $timestamp += int(rand()*$nint);
		if ($rnum < 0.05) {
			$nmaps = GetLength(70, $increment);
		} elsif ($rnum < 0.07) {
			$nmaps = GetLength(50, $increment);
		} elsif ($rnum < 0.11) {
			$nmaps = GetLength(30, $increment);
		} elsif ($rnum < 0.15) {
			$nmaps = GetLength(10, $increment);
		} else {
			$nmaps = int(rand()*10);
		}
		print $nmaps, "\t140\t60\t1\t15\t$timestamp\t0\n"
	}
	return 0;
}

Main(@ARGV);
#ParseRes(@ARGV);


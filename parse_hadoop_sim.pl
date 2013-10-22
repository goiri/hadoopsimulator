use strict;

sub Main {
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

Main(@ARGV);



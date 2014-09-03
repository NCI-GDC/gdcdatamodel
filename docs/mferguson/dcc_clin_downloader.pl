#!/usr/bin/perl

##########################################################################
#
# Make local file system copy of BCR XML files.  This only gets and puts the 
#	.tar.gz versions in the filesystem, and unpacks them into another directory.
#
# Version: 2014-01-29.1

##########################################################################
# Libraries that need to be included
use LWP::UserAgent;
use HTTP::Cookies;
use XML::LibXML;
use File::Copy;
use Log::Log4perl qw(:easy);
##########################################################################

##########################################################################
# First get some time / date stamps
# This text is used by the log file
$today = localtime();
chomp $today;

# This is used as a suffix on certain directory name(s)
$now = `date +"%Y%m%d%H%M%S"`;
chomp $now;
##########################################################################

##########################################################################
# Read in the users DCC password from a secure file.
# TODO this has be done more securely.
# This is no longer necessary, all BCR XML files moved to open access directory
#$password = ;
#chomp $password;
#$userId = "XXXXXXX";
##########################################################################

##########################################################################
# Following: some variables that must be set by the user

# Actually run the retrieve, and save the files
$execRetr = 1;
$execSave = 1;

# Write to a LOG file?
$log = 1;

# Various variables that should be set by the user
#
# tmp location for files to be downloaded, and log files
# Note: this location is not automatically created.  If it goes away
# it's an indication of system reboot, and makes me want to go in and
# check on stuff.
$tempLoc = "/tmp/tcga";

# Location in the filesystem for comprehensive *.tar.gz file set
$dataDirRoot = "/home/martin/my_documents/consulting_practice/NCI/TCGA/05_dcc";
# Location on NCI server:
#$dataDirRoot = "/data1/bcr";

$allArchsDir 		= "$dataDirRoot/all_archives_" . $now;
$allArchsUnpackDir 	= "$dataDirRoot/all_" . $now;
$latestArchsDir 	= "$dataDirRoot/latest_" . $now;

# TODO Log file location (make sure it exists and is writable)
$logFileDir = "$dataDirRoot/logs";

# !! Nothing below here to set for the casual user !!
##########################################################################

##########################################################################
# Other variables that could be tuned.

# DCC URL that returns XML list of all archives
# 
#$archiveListURL = 'http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=Archive&Archive[baseName=*_bio]';
$archiveListURL = 'http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=Archive&Archive[Platform[@name=bio]]&startIndex=0&resultCounter=200&pageSize=200';
#$archiveListURL = '/tmp/GetXML.xml';
#
# cache clearing URL, because of bug in caCore SDK. Call once, chuck results.
$clearCacheURL  = 'http://tcga-data.nci.nih.gov/tcgadccws/GetXML?query=Archive&Archive[Platform[@name=bio]]&startIndex=200&resultCounter=200&pageSize=200';

# xpath that finds all the nodes of archives in the above response
$archiveNodesXpath = '//class[@name="gov.nih.nci.ncicb.tcga.dccws.Archive"]';

# Stuff for logging
# Check / create the log file directory
if ( ! -e $logFileDir ) {
	mkdir $logFileDir;
}

# Make log file name
$lfn 		= "DCC_download_" . $now;
$logFname 	= "$logFileDir\/$lfn";
	
# Error file
$efn 		= "DCC_download_errors_" . $now;
$errorFname = "$logFileDir\/$efn";
	
# Open filehandle
#open(LOG, $logFname) || ERROR ("Can't open log file: $logFname: $!")  && die;
##########################################################################

##########################################################################
# Set up the logging environment. Normally set to $INFO ($DEBUG, $WARN, others)
Log::Log4perl->easy_init( $INFO );
# Capture the XML into text files
#$DEBUG2 = 1;

INFO ("Execute Retr is:    $execRetr\n");
INFO ("Execute Save is:    $execSave\n");
INFO ("Logging is:         $log\n");
INFO ("");
INFO ("Temp location:      $tempLoc\n");
INFO ("Data root dir:      $dataDirRoot\n");
INFO ("All archives dir:   $allArchsDir\n");
INFO ("All unpacked archs: $allArchsUnpackDir\n");
INFO ("Latest archs:       $latestArchsDir\n");
INFO ("Log file dir:       $logFileDir\n");
INFO ("Log file:           $logFname\n");
INFO ("Error file:         $errorFname\n");
INFO ("");
##########################################################################


############################## Program starts here ##########################
INFO ("0: Begin: dcc_clin_downloader - run date: $today"); 

# 0 #########################################################################
# Do some preparatory work: 
if (! -e "$dataDirRoot") {
	FATAL ("Data root directory does not exist: $dataDirRoot") && die("FATAL");
}

if (! -e "$allArchsDir") {
	# Make the new allArchsDir: e.g. /data1/bcr/all_archives_$now <- this is where *.tar.gz files go
	my $tempDir = $allArchsDir;
	$tempDir =~ s/\d+$/\*/;		# strip off date component
	$tempDir = glob ($tempDir);	# look for e.g. /data1/bcr/all_archives_* - assume there is 1 - bad assumption  TODO
	INFO ("\t0.1: Rename *.tar.gz directory: $tempDir -> $allArchsDir");
	move ("$tempDir", "$allArchsDir") || FATAL ("\tDie: All .tar.gz archive directory not created: $allArchsDir") && die("FATAL");
	
	DEBUG ("\t\tDelete old symlink: $dataDirRoot/all_archives");
	unlink ("$dataDirRoot/all_archives") || FATAL ("\tDie: Could not delete old symlink: $dataDirRoot/all_archives") && die("FATAL");

	($tempDir) = $allArchsDir =~ m/.*\/([^\/]+)$/;
	DEBUG ("\t\tMaking relative symlink: $tempDir -> $dataDirRoot/all_archives");
	# symlink OLDFILE,NEWFILE	
	symlink ("$tempDir", "$dataDirRoot/all_archives") || FATAL ("\tDie: Symlink not created: $dataDirRoot/all_archives") && die("FATAL");
}
	
if (! -e "$allArchsUnpackDir") {
	my $tempDir =  $allArchsUnpackDir;
	$tempDir =~ s/\d+$/\*/;
	$tempDir = glob ($tempDir);	
	INFO ("\t0.2: Rename upacked directory: $tempDir -> $allArchsUnpackDir");
	move ("$tempDir", "$allArchsUnpackDir") || FATAL ("\t\tAll unpacked archive directory not created: $allArchsUnpackDir") && die("FATAL");
	
	DEBUG ("\t\tDelete old symlink: $dataDirRoot/all");
	unlink ("$dataDirRoot/all") || FATAL ("\t\tDie: Could not delete old symlink: $dataDirRoot/all") && die("FATAL");
	
	($tempDir) = $allArchsUnpackDir =~ m/.*\/([^\/]+)$/;
	DEBUG ("\t\tMaking relative symlink: $tempDir -> $dataDirRoot/all");
	# symlink OLDFILE,NEWFILE
	symlink ("$tempDir", "$dataDirRoot/all") || FATAL ("\t\tDie: Symlink not created: $dataDirRoot/all") && die("FATAL");
}

# Die if temporary location does not exist already. Indication of system reboot, so need to check stuff.
INFO ("\t0.3: Checking for existence of: $tempLoc");
if (! -e "$tempLoc") {
	FATAL ("Die: Temporary download directory does not exist: $tempLoc") && die("FATAL");
}

# Check that DCC REST service is working and clear cache at same time
INFO ("\t0.4: Testing DCC REST service, clearing cache: $clearCacheURL");
DEBUG ("\t\tRetrieving clear cache URL: $clearCacheURL");
# Create a new XML parser, set the recovery mode re. errors
$parser0 = XML::LibXML->new();
$parser0->recover(2);

eval {
	$testDoc = $parser0->parse_file($clearCacheURL);    # TODO deal with bad response, put into eval {} block
};
if ($@) {
	FATAL ("\t\tDie: Retrieving failed for: $clearCacheURL\n");
	FATAL ("\t\t     $@\n") && die("FATAL");
} else {
	DEBUG ("\t\tURL retrieved.");
	$testRoot        = $testDoc->documentElement();
	$testRecordCount = $testRoot->findvalue('//queryResponse/recordCounter[1]');
	$testFirstRecord = $testRoot->findvalue('//queryResponse/class[1]/@recordNumber');
	$testLastRecord  = $testRoot->findvalue('//queryResponse/class[last()]/@recordNumber');
	$nextURL         = $testRoot->findvalue('//queryResponse/next[1]/@xlink:href');
	DEBUG("\t\tRecord count: $testRecordCount\tStart: $testFirstRecord\tEnd: $testLastRecord\tNextURL: $nextURL");
	if (! $testRecordCount) {
		FATAL ("\t\tDie: Did not get valid record count.\n") && die("FATAL");
	} elsif ($testFirstRecord != 201) {
		FATAL ("\t\tDie: Wrong first record number.\n") && die("FATAL");
	} elsif ($testLastRecord != 400 ) {
		FATAL ("\t\tDie: Wrong last record number.\n") && die("FATAL");
	} elsif ($nextURL !~ m/http:.*&startIndex=400&resultCounter=200/) {
		FATAL ("\t\tDie: Wrong next page URL.\n") && die("FATAL");
	}
}

# End preparatory work
#############################################################################


# 1 #########################################################################
# Request DCC REST list of archives, Loop over DCC archive request URL, there are likely multiple chunks

# Create a new XML parser, set the recovery mode re. errors
my $parser = XML::LibXML->new();
$parser->recover(2);
my $totalArchives = 0;

INFO ("1: Checking DCC for complete list of BCR archives:\n");
while (1) {
	DEBUG ("\t\tRetrieving URL: $archiveListURL");
	eval {
		$doc = $parser->parse_file($archiveListURL);    # TODO deal with bad response, put into eval {} block
	};
	if ($@) {
		FATAL ("\t\tDie: Retrieving failed for: $archiveListURL\n");
		FATAL ("\t\t     $@\n") && die("FATAL");
	}
	
	$root = $doc->documentElement();
	if ( !$totalArchives ) {
		$totalArchives = $root->findvalue('//queryResponse/recordCounter[1]');
		if ( !$totalArchives ) {
			FATAL ("\t\tDie: No archives in DCC response") && die("FATAL");
		}
		INFO ("\t1.1: Total achives in DCC response:           $totalArchives");
	}

	push( @nodes, $root->findnodes($archiveNodesXpath) );
	DEBUG ("\t\tTotal nodes in DCC XML so far:        ", scalar @nodes);
	
	if ($DEBUG2) {
		DEBUG ("\t\tCapturing full XML responses to log.");
		# Save the XML to a file
		$firstRecord = $root->findvalue('//queryResponse/class[1]/@recordNumber');
		$lastRecord  = $root->findvalue('//queryResponse/class[last()]/@recordNumber');
		DEBUG ("\t\t\tFirst record: $firstRecord\tLast record: $lastRecord");
		
		$xmlFilename = $now . "_xml_" . $firstRecord . "_to_" . $lastRecord . "_of_" . scalar @nodes . "_of_" . $totalArchives . ".xml";
		DEBUG ("\t\t\tFilename: $xmlFilename");
		$xmlFilename = "$logFileDir/$xmlFilename";
		DEBUG ("\t\t\tFilename: $xmlFilename");
		open(XMLLOG, ">$xmlFilename")  || FATAL ("\t\t\tCould not open XML capture log file: $xmlFilename");
		print XMLLOG $doc->toString(2);
		close(XMLLOG);
	}
	
	# Get the next URL from the bottom of the current XML.
	if ( (my $node) = $root->findnodes('//queryResponse/next[1]') ) {
		$archiveListURL = $node->getAttribute("xlink:href");
		DEBUG ("\t\tNext URL: $archiveListURL");
		$archiveListURL = $archiveListURL . '&pageSize=200';
		DEBUG ("\t\tMunged next URL: $archiveListURL");
	}
	else {
		DEBUG ("\t\tThat was last URL.");
		last;
	}
}
INFO ("\t1.2: Total archive nodes successfully parsed: ", scalar @nodes, "\n");
if (scalar @nodes < $totalArchives) {
	FATAL ("\t1.3: Die: Number of nodes parsed is less than total nodes in response.") && die("FATAL");
}
#############################################################################


# 2 #########################################################################
# Go over each node.  Make a hash of every batch / revision with cancer + archiveName + uploadDate
$badArchives = $badLocation = $level2Archives = $noWantArchives = $goodArchives = 0;
$availArchives = $notAvailArchives = 0;
$goodArchivesAvail = 0;

INFO ("2: Check each archive's DCC metadata:");
foreach $node (@nodes) {
	($bcr, $cancer)	= $node->findvalue('field[@name="baseName"]') =~ m/([\w\.]+)_(\w+)_bio/;
	$name			= $node->findvalue('field[@name="name"]');
	$batch    		= $node->findvalue('field[@name="serialIndex"]');
	$revision 		= $node->findvalue('field[@name="revision"]');
	$isAvail 		= $node->findvalue('field[@name="deployStatus"]');		
	$archiveUrl 	= $node->findvalue('field[@name="deployLocation"]');
	$isLatest 		= $node->findvalue('field[@name="isLatest"]');
	$date 			= $node->findvalue('field[@name="addedDate"]');	
	
	# Expand location to full URL
	$archiveUrl = "https://tcga-data.nci.nih.gov" . $archiveUrl;
	# Get the archive name from the URL
	($archName) = $archiveUrl =~ m/.*\/([^\/]+)/;

	######################
	my $noWantArchive = 0;
	# A sequential set of filters to kick out nodes that do not represent Level 1 Biospecimen or Clinical XML archives from a BCR 
	if ( !$cancer ) {
		$badArchives++;
		$cancer = "NOTCANCER";
		$noWantArchive = 1;
		DEBUG("\t\tNOT CANCER. CountBad: $badArchives: $cancer $batch $revision $name");
		#next;
	}
	
	if ($archiveUrl =~ m/deposit_ftpusers/) {
		# Bad location URL - still in deposit directory
		$badLocation++;
		$noWantArchive = 1;
		DEBUG("\t\tWRONG LOCATION: NOT IN DCC Deposit Directory. CountBadLoc: $badLocation: $cancer $batch $revision $name");
		#next;
	}
	
	if ($archiveUrl =~ m/\.Level_2\./) {
		# Not a Level 1 archive
		$level2Archives++;
		$noWantArchive = 1;
		DEBUG("\t\tWRONG LEVEL: CountWrongLevel: $level2Archives: $cancer $batch $revision $name");
		#next;
	}
	if ($noWantArchive) {
		$noWantArchives++;
		$archives{$cancer}{$batch}{$revision}{'isgood'} = 0;
	} else {
		$goodArchives++;
		$archives{$cancer}{$batch}{$revision}{'isGood'} = 1;	
	}
	#######################
	
	#######################
	my $availArchive;
	# Availability is tracked separately from being good
	if ($isAvail ne "Available") {
		# This archive is not available
		$availArchive = 0;		
		$notAvailArchives++;
		$archives{$cancer}{$batch}{$revision}{'isAvail'}  = 0;
		DEBUG("\t\tNOT AVAIL: $isAvail. CountNotAvail: $notAvailArchives: $cancer $batch $revision $name");
		#next;
	} else {
		$availArchives++;
		$availArchive = 1;		
		$archives{$cancer}{$batch}{$revision}{'isAvail'}  = 1;
	}
	#######################
	
	$archives{$cancer}{$batch}{$revision}{'location'} = $archiveUrl;
	$archives{$cancer}{$batch}{$revision}{'archName'} = $archName;
	$archives{$cancer}{$batch}{$revision}{'date'}     = $date;
	$archives{$cancer}{$batch}{$revision}{'isLatest'} = $isLatest;
	
	if ($availArchive && ! $noWantArchive) {
		$goodArchivesAvail++;
		
		$archives{$cancer}{$batch}{$revision}{'goodAvail'} = 1;
	
		DEBUG ("\t\tARCH GOOD & AVAIL. CountGood: $goodArchives: $archName Date: $date Latest: $isLatest Available: $isAvail $name");
	
		# TODO Get the BCR name

		# TODO Fix the date

		#$nodeID = $node->findvalue('field[@name="id"]');
	}
}

INFO ("\t2.0: Total archives:                          $totalArchives");
INFO ("\t2.1: Bad archives:                            $badArchives");
INFO ("\t2.2: Level_2 archives:                        $level2Archives");
INFO ("\t2.3: Wrong location archives:                 $badLocation");
INFO ("\t2.4: No want archives - total:                $noWantArchives");
INFO ("\t2.5: Good archives - total:                   $goodArchives");
INFO ("\t2.6: DCC unavailable archives:                $notAvailArchives");
INFO ("\t2.7: DCC available archives:                  $availArchives");
INFO ("\t2.8: Union: Good & Available archives:        $goodArchivesAvail");
# End loop for retrieving DCC node list of all archives
##########################################################################


# 3 ########################################################################
# Check list of archives from DCC against the local files, mod the hash if I have it already.
$FShave = $FShaveB4Retr = $FSnoHaveNoCanGet = $FSnoHaveGet = 0;
INFO ("3: Check each archive against local TCGA files:");

# Get array all .tar.gz archives in the all archive directory
@allArchsB4Retr = retrieveTGZlist($allArchsDir);
$FShaveB4Retr = scalar @allArchsB4Retr;
INFO ("\t3.1: All Bio/Clin XML *.tar.gz archives already in local FS: $FShaveB4Retr");

# Go through the hash
foreach $cancer ( sort keys %archives ) {
	foreach $batch ( sort { $a <=> $b } keys %{ $archives{$cancer} } ) {
		foreach $revision ( sort { $a <=> $b } keys %{ $archives{$cancer}{$batch} } ) {
			
			# Skip files that are not good
			next if (! $archives{$cancer}{$batch}{$revision}{'isGood'});
			
			my $testFname = "$allArchsDir/" . $archives{$cancer}{$batch}{$revision}{'archName'};
			DEBUG ("\t\tTesting for: $testFname");
			if ( -e "$testFname" ) {
				# I have it
				$archives{$cancer}{$batch}{$revision}{'FShave'} = 1;
				$archives{$cancer}{$batch}{$revision}{'getIt'}  = 0;
				DEBUG ("\t\t\tHave already: $cancer $batch $revision\n");
				$FShave++;
				# Count have it vs. availability
				if ($archives{$cancer}{$batch}{$revision}{'isAvail'}) {
					$FShaveAvail++;
				} else {
					$FShaveNotAvail++;
				}
			} elsif (! $archives{$cancer}{$batch}{$revision}{'isAvail'}) {
				# I don't have it and CANNOT get
				$archives{$cancer}{$batch}{$revision}{'FShave'} = 0;
				$archives{$cancer}{$batch}{$revision}{'getIt'}  = 0;
				WARN ("\t\tWanted, don't have, can't get: $cancer $batch $revision $archives{$cancer}{$batch}{$revision}{'date'} $archives{$cancer}{$batch}{$revision}{'archName'}\n");
				$FSnoHaveNoCanGet++;
			} else {
				# I don't have it, it is available, so GET it
				$archives{$cancer}{$batch}{$revision}{'FShave'} = 0;
				$archives{$cancer}{$batch}{$revision}{'getIt'}  = 1;
				DEBUG ("\t\t\tDon't have, must get: $cancer $batch $revision\n");
				$FSnoHaveGet++;				
			}
		}
	}
}

INFO ("\t3.2: All Bio/Clin *.tar.gz archives wanted, already have:    $FShave");
INFO ("\t3.3: Wanted, have, but not available anymore:                $FShaveAvail");
INFO ("\t3.4: Wanted, have, and still available:                      $FShaveNotAvail");

# TODO what I have but DCC doesn't know about anymore
INFO ("\t3.5: Wanted, don't have, but can't get:                      $FSnoHaveNoCanGet");
INFO ("\t3.6: Wanted, don't have, and can get:                        $FSnoHaveGet");
##########################################################################


# 4 ########################################################################
# Get the archives I don't have, and put them in the filesystem
$need = $retrieved = $retrieveOK = $retrieveAlreadyHad = $retrieveFail = $savedOK = $retrieveAttempt = $savedFail = $notTriedRetrieve = $retrieveFailed = 0;
INFO ("4: Get the archives I am missing:");
WARN ("\tEXECUTE RETRIEVE MODE: $execRetr") if ! $execRetr;
WARN ("\tEXECUTE SAVE MODE: $execSave") if ! $execSave;

foreach $cancer ( sort keys %archives ) {
		foreach $batch ( sort { $a <=> $b } keys %{ $archives{$cancer} } ) {
			foreach $revision ( sort { $a <=> $b } keys %{ $archives{$cancer}{$batch} } ) {
				$shouldHave++;
				if ( $archives{$cancer}{$batch}{$revision}{'getIt'} ){
					INFO ("\t\tNeed to retrieve: $archives{$cancer}{$batch}{$revision}{'location'}");
					$need++;
					my $testFname = "$tempLoc/" . $archives{$cancer}{$batch}{$revision}{'archName'};
					if ( -e "$testFname" ) {
						# We have the file already in the temp hold location
						INFO ("\t\t\tAlready have in $tempLoc: $testFname");
						$retrLocation = "$testFname";
						$retrieveAlreadyHad++;
						$retrSuccess = 1;
					} elsif ($execRetr) {
						$retrieveAttempt++;
						# Execute the retrieval attempt
						INFO ("\t\t\tRetrieving: $archives{$cancer}{$batch}{$revision}{'location'}");
						($retrSuccess, $retrLocation, $retrDate) = retrieveDCCarchive($archives{$cancer}{$batch}{$revision}{'location'});
						if ($retrSuccess) {
							$retrieved++;
						} else {
							$retrieveFailed++
						}
					} else {
						# Don't execute retrieval attempt - test mode
						$retrSuccess = 0;
						WARN("\t\t\tNO Retreive attempt made - test mode.");
						$notTriedRetrieve++;
					}
					
					# Successfully retrieved or had file. Move file to final location. Account.
					if ($retrSuccess) {
						INFO ("\t\t\tOK Retrieve or Have Already: $archives{$cancer}{$batch}{$revision}{'archName'}");
						$retrieveOK++;
						INFO ("\t\t\tMoving file to archive dir: $retrLocation -> $allArchsDir");
						if ($execSave) {
							# move it to allArchive dir
							move($retrLocation, $allArchsDir) || FATAL ("\t\tMove failed: $!") && exit(1);
							DEBUG ("\t\t\t\tMove OK");
							$archives{$cancer}{$batch}{$revision}{'newArch'} = 1;
							$archives{$cancer}{$batch}{$revision}{'FShave'} = 1;
							$archives{$cancer}{$batch}{$revision}{'FSlocation'} = "$allArchsDir/$archives{$cancer}{$batch}{$revision}{'archName'}";
							$savedOK++;
							$FShave++;
						} else {
							DEBUG ("\t\t\t\tNo move - Not EXECing save function");
							$savedFail++;
						}
					} else {
						WARN ("\t\t\tRetrieve Failed");
						$retrieveFail++;
						$savedFail++;
					}
				} else {
					DEBUG ("\t\tDon't try to retrieve: $archives{$cancer}{$batch}{$revision}{'location'}");
					$had++;
				}
			}
		}
}
INFO ("\t4.1:  Archives should have:                    $shouldHave");
INFO ("\t4.2:  Archives already have:                   $had");
INFO ("\t4.3:  Archives needed:                         $need");
INFO ("\t4.4:  Archives already had $tempLoc:          $retrieveAlreadyHad");
INFO ("\t4.5:  Archives retrieve attempt made:          $retrieveAttempt");
INFO ("\t4.6:  Archives retrieve failed:                $retrieveFail");
INFO ("\t4.7:  Archives successfully retrieved:         $retrieved");
INFO ("\t4.8:  Archives no retrieve attempted:          $notTriedRetrieve");
INFO ("\t4.9:  Archives successfully retrieved or had:  $retrieveOK");
INFO ("\t4.10: Archives successfully saved:             $savedOK");
INFO ("\t4.11: Have archives in local FS:               $FShave");
INFO ("\t4.12: Archives not saved:                      $savedFail");
#########################################################################


# 5 #########################################################################
# Unpack the tar.gz files into the unpacked directory
INFO ("5: Unpack (tar -xzf) any .tar.gz file not already unpacked:");
$FShave = $needToUnpack = $unpackedAlready = $unpackNotAttempted = $unpackedOK =  $unpackedFail = 0;

# Compare contents of allArchsDir with allArchsUnpackedDir
# Get post-retrieve list of archives in local directory.
@allArchs = retrieveTGZlist($allArchsDir);
$FShave = scalar @allArchs;
INFO ("\t5.1: Now have *.tar.gz archives in local FS:  $FShave");

foreach $arch (sort @allArchs) {
	my $testDirName = $arch;
	$testDirName =~ s/\.tar\.gz$//;
	DEBUG ("\t\tTesting for unpacked dir: $allArchsUnpackDir/$testDirName");
	# TODO: need to also check that directory is not empty
	if (! -e "$allArchsUnpackDir/$testDirName") {
		# Untar zip the original into here
		DEBUG ("\t\t\tNeed to unpack: $allArchsDir/$arch");
		$needToUnpack++;
		if ($execSave) {
			INFO ("\t\t\tExecuting unpack on: $allArchsDir/$arch");
			system('tar', '-xzf', "$allArchsDir/$arch", '-C', "$allArchsUnpackDir");
			if ( $? == -1 ) {
  				WARN ("\t\t\tUntar failed: $! : $allArchsDir");
  				$unpackedFail++;
  				die ('FATAL');
			} else {
				my $val = $? >> 8;
  				DEBUG ("\t\t\tUntar successful: exited with value: $val");
				$unpackedOK++;
			}
		} else {
			$unpackNotAttempted++;
		}
	} else {
		DEBUG ("\t\t\tAlready unpacked");
		$unpackedAlready++;
	}
}
INFO ("\t5.2: Already unpacked:                        $unpackedAlready");
INFO ("\t5.2: Need to have unpacked:                   $needToUnpack");
INFO ("\t5.3: Unpacked this run:                       $unpackedOK");
INFO ("\t5.3: Unpacked fail:                           $unpackedFail");
INFO ("\t5.6: Unpack not attempted:                    $unpackNotAttempted");
##################################################################################


# 6 ##############################################################################
# Create suddirectory named latest_<timestamp> with isLatest symlinked to latest copy in allArchivesUnpackedDir
INFO ("6: Create subdirectory with symlinks to latest unpacked:");

if ( ! -e  "$latestArchsDir" ) {
	INFO ("\t6.1: Creating subdirectory: $latestArchsDir");
	mkdir ("$latestArchsDir") || FATAL ("\t\tCould not create subdirectory") && die("FATAL");
	INFO ("\t6.2: Symlinking: latest ->  $latestArchsDir");
	unlink ("$dataDirRoot/latest") || FATAL ("\tCould not delete old symlink: $dataDirRoot/latest") && die("FATAL");
	(my $tempDir) = $latestArchsDir =~ m/\/([^\/]+)$/;
	symlink("$tempDir","$dataDirRoot/latest") || FATAL ("\tSymlink not created: $dataDirRoot/latest") && die("FATAL");
}

# Iterate over unpacked directories, pick latest, symlink it to a pointer in latest_<now>
$symlinksCreated = $symlinksNotCreated = $flagged = $symlinksAlreadyThere = 0;

# 1 - Get list of all directories, based on all archives, in $allArchsUnpackDir
@allUnpackArchs = retrieveTGZlist($allArchsDir);
@allUnpackArchs = map {$_ =~ s/\.tar\.gz$//; $_} @allUnpackArchs; # This substitution just gets rid of the .tar.gz
$FShave = scalar @allUnpackArchs;
INFO ("\t6.3: Now have *.tar.gz archives in local FS:  $FShave");

# 2 - Iterate - check if it is in %archives
foreach $unpackArch (@allUnpackArchs) {
	(my $cancer, my $batch, my $revision) = $unpackArch =~ m/_([A-Z]+)\.bio\.(?:Level_\d\.)?(\d+)\.(\d+)\.\d+$/;
	next if ($level eq "Level_2");
	if ( $archives{$cancer}{$batch}{$revision} ) {
		# If in %archives
		if ($archives{$cancer}{$batch}{$revision}{'isLatest'}) {
			# If is latest, symlink
			my $linkName = $archives{$cancer}{$batch}{$revision}{'archName'};
			$linkName =~ s/\.tar\.gz$//;
			DEBUG ("\t\tCreating symlink: $linkName -> $allArchsUnpackDir/$linkName");
			if (! -l $linkName ) {
				symlink("../all/$linkName","$latestArchsDir/$linkName");
				$symlinksCreated++;
			} else {
				$symlinksAlreadyThere++;
			}
		} else {
			# If not latest, skip
			$symlinksNotCreated++;
			next;
		}
	} else {
		# If not in %archives, flag. Get next pass
		DEBUG ("\t\t\tFlag for next pass: $cancer $batch $revision");
		`echo "$cancer $batch $revision" >> /tmp/flag.txt`; 
		$flagged++;
	}
}
INFO ("\t6.4: Symlinks not created:                    $symlinksNotCreated");
INFO ("\t6.5: Symlinks created:                        $symlinksCreated");
INFO ("\t6.6: Symlinks already there:                  $symlinksAlreadyThere");
INFO ("\t6.7: Archives flagged:                        $flagged");
##########################################################################

exit(0);

# End of program
##########################################################################


sub retrieveDCCarchive {
	# New useragent and credentials
	$ua = LWP::UserAgent->new;
	# 2012-06: Don't need credentials anymore.  All archived in open access tier.
	# $ua->credentials( 'tcga-data-secure.nci.nih.gov:443', 'TCGA DATA Secure Download', $userId, $password );

	my $loc = pop@_;
	
	# Get the filename part of the location
	(my $name) = $loc =~ m/.*\/([^\/]+)$/;
	
	# Test that file does not already exist in temp location.  If does, just pretend to get it.
	DEBUG ("\t\t\t\tSUB: Testing for existence: $tempLoc/$name");
	if (! -e "$tempLoc/$name") {
		DEBUG ("\t\t\t\tSUB: Retrieve file: File not in $tempLoc: $loc");
	
		# Get file from location ($loc)
		$res = $ua->get( "$loc" );
		
		if ( $res->is_success ) {
			$success = $res->is_success;
			DEBUG ("\t\t\t\tSUB: File retrieved result: $success");
			
			$retrName = $res->filename;
			INFO ("\t\t\t\tSUB: Filename retrieved: $retrName");
			if ($name ne $retrName) {
				FATAL("\t\t\t\tSUB: Did not retrieve what was asked for: $name but got $retrName");
				die;
			}

			# TODO pull out Last-Modified: Wed, 25 Aug 2010 21:50:54 GMT from response
			$DCCfileDate = "";
		
			open (DUMP, "> $tempLoc/$name") || FATAL ("SUB: RETRIEVE FAILED: $name $!") && die("FATAL");
			$bytesWritten = syswrite( DUMP, $res->content, length( $res->content ) );
			if ( $bytesWritten != length( $res->content ) ) {
				close DUMP;
				FATAL ("SUB: Failed to write all bytes: $tempLoc/$name $1");
				FATAL("Fatal error: incomplete file save.");
				die;
			}
			close DUMP;
			DEBUG ("\t\t\t\tSUB: File saved OK: $tempLoc/$name $bytesWritten");
			return ($success, "$tempLoc/$name", $DCCfileDate);
		} else {
			WARN( "\t\t\tSUB: Bad response: " . $res->status_line . "\n");
			
		}
	} else {
		DEBUG ("\t\t\t\tSUB: No Retrieve - already have file in: $tempLoc");
		$success = 1;
		return ($success, "$tempLoc/$name", $DCCfileDate);
	}
}

# Return a list of *.tar.gz files matching BCR filename spec
sub retrieveTGZlist {
	
	my $location = pop @_;
	DEBUG ("\tSUB Checking for *.tar.gz files in: $location");
	
	opendir(DIR, "$location");
	my @tempFiles= readdir(DIR);
	close (DIR);
	
	my $count = scalar @tempFiles;
	DEBUG ("\t\tSUB Found total files: $count");
	
	#my %files;
	my @files;
	foreach $tempFile (@tempFiles) {
		chomp $tempFile;
		next if (! $tempFile);
		if ($tempFile !~ m/\.bio\.(?:Level_1\.)?\d+\.\d+\.\d+\.tar\.gz$/) {
			DEBUG ("\t\tSUB BAD TGZ FILE: $tempFile");
			next;	
		}
		
		#(my $cancer, my $batch, my $rev) = $tempFile =~ m/_([A-Z]+)\.bio\.(?:Level_1\.)?F(\d+)\.(\d+)\.\d+\.tar\.gz$/;
		#DEBUG ("\t\tSUB DIRECTORY LIST HAVE: $cancer $batch $rev $tempFile\n");
		
		#$files{$batch}{$rev}{'cancer'} 	= $cancer;
		#$files{$batch}{$rev}{'file'} 	= $tempFile;
		
		push (@files, $tempFile);
	}
	$count = scalar @files;
	DEBUG ("\t\tSUB Found bio/clin .tar.gz files: $count");
	
	return @files;
}
exit();


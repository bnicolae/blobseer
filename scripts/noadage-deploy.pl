#!/usr/bin/perl -w
use File::Path;
use File::Basename;
use Getopt::Long;

# global settings
$WORKING_DIR = $ENV{'PWD'};
$LOGIN_NAME = $ENV{'LOGNAME'};
$HOME_DIR = $ENV{'HOME'};

# template settings
$TEMPLATE_DIR = "$WORKING_DIR/templates";
$BAMBOO_TEMPLATE_FILE = "$TEMPLATE_DIR/bamboo_node_tweak.cfg";
$DHT_TEMPLATE_FILE = "$TEMPLATE_DIR/dht_template.cfg";

# run configuration 
$DHT_RUN = "$HOME_DIR/work/blobseer/trunk/sdht/sdht";
$PROVIDER_RUN = "$HOME_DIR/work/blobseer/trunk/provider/provider";
$PUBLISHER_RUN = "$HOME_DIR/work/blobseer/trunk/pmanager/pmanager";
$LOCKMGR_RUN = "$HOME_DIR/work/blobseer/trunk/vmanager/vmanager";
$DEPLOY_SCRIPT = "$TEMPLATE_DIR/deploy-process.sh";

# port configuration
$BAMBOO_RPC_PORT = 5852;
$BAMBOO_ROUTER_PORT = 5850;

############################################# HELPER FUNCTIONS #####################################

sub replace_var {
    $_[0] =~ s/\$\{$_[1]\}/$_[2]/g || print 'WARNING: parameter ${', $_[1], '} not found', "\n";
}

sub read_template {
    my $template = "";
    open(F_IN, "<$_[0]") || die "Configuration template $_[0] missing";
    $template = join('', <F_IN>);
    close(F_IN);
    return $template;
} 

# get the host list from the reservation
sub get_hosts {
    if ($_[0] eq '') {
	open(OARSTAT_PIPE, "oargridstat -l $job_id | uniq|");
    } else {
	open(OARSTAT_PIPE, "oargridstat -l $job_id | uniq | grep $_[0]|");
    }
    my @hosts = <OARSTAT_PIPE>;
    foreach(@hosts) {
	$_ =~ s/\n//;
    } 
    pop(@hosts);
    return @hosts; 
}

########################################### CFG FILE GENERATION #####################################

sub gen_bamboo_config {
    my $template = read_template($BAMBOO_TEMPLATE_FILE);
    my @gwl = @{$_[2]};
    my $gateway_count = $_[1];

    replace_var($template, 'host', $_[1]);
    replace_var($template, 'router_port', $BAMBOO_ROUTER_PORT);
    replace_var($template, 'gateway_count', $gateway_count);
    my $gateways = "";
    for(my $i = 0; $i < $gateway_count; $i++) {
	$gateways = $gateways."\t\tgateway_$i    $gwl[$i]:$BAMBOO_ROUTER_PORT\n";
    }
    replace_var($template, 'gateway_list', $gateways);
    replace_var($template, 'rpc_port', $BAMBOO_RPC_PORT);
    replace_var($template, 'home_dir', "/tmp/$LOGIN_NAME/$_[0]/bamboo/sm-blocks-0");

    open(BAMBOO_OUT, ">$WORKING_DIR/$_[0]/bamboo_$_[1].cfg");
    print BAMBOO_OUT $template;
    close(BAMBOO_OUT);
}

sub gen_dht_config {
    my $template = read_template($DHT_TEMPLATE_FILE);
    my @gwl = @{$_[0]};
    my $no_gw = $_[1];

    my $gateways = "\n\t\t".join(",\n\t\t", map("\"$_\"", @gwl[2..$no_gw - 1]))."\n\t";

    replace_var($template, 'gateways', $gateways);
    replace_var($template, 'pmanager', "\"".$gwl[0]."\"");
    replace_var($template, 'vmanager', "\"".$gwl[1]."\"");

    open(TEST_OUT, ">$CFG_FILE");
    print TEST_OUT $template;
    close(TEST_OUT);
}

#############################################  DIRECT DEPLOYMENT  ##############################################

sub deploy_process {
    my $hostname = $_[0];
    my($filename, $pathname, $suffix) = fileparse($_[1]);
    my $cmdname = $filename.$suffix;
    my $args = "\'$_[2]\'";
    my $dest_dir = $_[3];
    
    `oarsh $hostname \"env CLASSPATH=$ENV{'CLASSPATH'} LD_LIBRARY_PATH=$ENV{'LD_LIBRARY_PATH'} $DEPLOY_SCRIPT $pathname $cmdname $args $dest_dir\" >/dev/null`;
    # Testing
    # `ssh $hostname \"$DEPLOY_SCRIPT $dirname $basename $args $dest_dir\" >/dev/null`;
    $? == 0 || die "Command failed: oarsh $hostname \"$DEPLOY_SCRIPT $pathname $cmdname $args $dest_dir\" >/dev/null";
    print "success for $cmdname\n";
}

sub deploy_manually {
    my $job_id = $_[0];
    my @hosts = @{$_[1]};
    my $dht_hosts = $_[2];
    my $provider_hosts = $_[3];
    deploy_process($hosts[0], $PUBLISHER_RUN, $CFG_FILE, "/tmp/$LOGIN_NAME/$job_id/pmanager");
    deploy_process($hosts[1], $LOCKMGR_RUN, $CFG_FILE, "/tmp/$LOGIN_NAME/$job_id/vmanager");
    for ($i = 2; $i < $dht_hosts; $i++) {
	print "Now processing dht host $hosts[$i] ($i/$no_hosts)...";
	deploy_process($hosts[$i], $DHT_RUN, $CFG_FILE, "/tmp/$LOGIN_NAME/$job_id/dht");
    }
    for ($i = 2; $i < $provider_hosts; $i++) {
	print "Now processing provider host $hosts[$i] ($i/$no_hosts)...";
	deploy_process($hosts[$i], $PROVIDER_RUN, $CFG_FILE, "/tmp/$LOGIN_NAME/$job_id/provider");
    }
}

sub kill_manually {
    my @hosts = @{$_[0]};
    my $no_hosts = @hosts;
    for($i = 0; $i < $no_hosts; $i++) {
	print "Now processing host $hosts[$i] ($i/$no_hosts)...";
	`oarsh $hosts[$i] \"killall -u $LOGIN_NAME\"`;
	if ($? == 0) { print "OK!\n"; } else { print "no owned processes\n"; }
    }
}

sub getstatus_manually {
    my @hosts = @{$_[0]};
    my $no_hosts = @hosts;
    for($i = 0; $i < $no_hosts; $i++) {
	print "Processes running on $hosts[$i] ($i/$no_hosts)...\n";
	my $pscmd = "ps aux | grep provider | grep -v grep;".
	    "ps aux | grep pmanager | grep -v grep;".
	    "ps aux | grep vmanager | grep -v grep;".
	    "ps aux | grep sdht | grep -v grep;".
	    "ps aux | grep bamboo.lss.DustDevil | grep -v grep";
	print `oarsh $hosts[$i] \"$pscmd\"`;
    }
}

#################################################  MAIN PROGRAM  ###############################################

# get params 
$usage = "Usage: noadage-deploy.pl -job <oar_job_id> [-kill | -status] [-dht n] [-providers n] [-cluster <cluster_name>]";
$job_id = 0;
$dht_hosts = 0;
$provider_hosts = 0;
$cluster_name = '';
GetOptions('job=i' => \$job_id, 
           'kill' => \$kill_flag, 
	   'status' => \$check_flag,
	   'dht=i' => \$dht_hosts,
	   'providers=i' => \$provider_hosts,
	   'cluster=s' => \$cluster_name
	  ) || die $usage;
if ($job_id == 0) { die $usage; }
$ENV{'OAR_JOB_KEY_FILE'} = "/tmp/oargrid/oargrid_ssh_key_".$LOGIN_NAME."_$job_id";

# Get hosts
@hosts = get_hosts($cluster_name);
#@hosts = ("paramount1", "paramount2", "paramount3");
if (@hosts == 0) { die "Could not parse the reservation host list\nMake sure job ID is valid & you are running on frontend"; }
$no_hosts = @hosts;
printf "oar2 reports $no_hosts reserved nodes for $job_id\n";
if ($no_hosts < 3) { die "FATAL: Must reserve at least 3 nodes"; } 
if ($dht_hosts == 0) { $dht_hosts = $no_hosts - 2; }
if ($provider_hosts == 0) { $provider_hosts = $no_hosts - 2; }

if ($kill_flag) {
    kill_manually(\@hosts);
    exit(0);
}
if ($check_flag) {
    getstatus_manually(\@hosts);
    exit(0);
}

# create directories 
$dir = $job_id;
rmtree([$dir], 0, 1);
mkdir($dir);

$CFG_FILE = "$WORKING_DIR/$job_id/test.cfg";
gen_dht_config(\@hosts, $dht_hosts);
print "Generated configuration file successfully for $no_hosts nodes\n";

# Deployment
print "Starting deployment...\n";
deploy_manually($job_id, \@hosts, $dht_hosts, $provider_hosts);
print "Deployment suceessful\n";

# Postprocessing
#print 'Postcleanup...', "\n";
#rmtree([$dir], 0, 1);

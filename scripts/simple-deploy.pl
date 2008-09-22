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

# additional settings
$PROVIDER_PAGE_NO = 32768;

# run configuration 
#$BAMBOO_RUN = "$TEMPLATE_DIR/run_bamboo.sh";
$BAMBOO_RUN = "$HOME_DIR/work/dhtmem/sdht/sdht";
$PROVIDER_RUN = "$HOME_DIR/work/dhtmem/provider/provider";
$PUBLISHER_RUN = "$HOME_DIR/work/dhtmem/publisher/publisher";
$LOCKMGR_RUN = "$HOME_DIR/work/dhtmem/lockmgr/lock_mgr";
$DEPLOY_SCRIPT = "$TEMPLATE_DIR/deploy-process.sh";

# port configuration
#$BAMBOO_RPC_PORT = 5852;
$BAMBOO_RPC_PORT = 5801;
$BAMBOO_ROUTER_PORT = 5850;
$PROVIDER_PORT = 5800;
$PUBLISHER_PORT = 5801;
$LOCKMGR_PORT = 5802;

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
    open(OARSTAT_PIPE, "oargridstat -l $job_id | uniq|");
    my @hosts = <OARSTAT_PIPE>;
    foreach(@hosts) {
	$_ =~ s/\n//;
    } 
    pop(@hosts);
    return @hosts; 
}

# get the host list from input file
sub get_hosts_file {
    open(HOST_FILE, $_[0]);
    my @hosts = <HOST_FILE>;
    foreach(@hosts) {
	$_ =~ s/\n//;
    } 
    pop(@hosts);
    return @hosts;
}

########################################### CFG FILE GENERATION #####################################

sub gen_bamboo_config {
    my $template = $BAMBOO_TEMPLATE;
    my @gwl = @{$_[2]};
    my $gateway_count = @gwl;

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

sub gen_provider_config {
    my $template = "provider: {\n".
	"\thost = \"$_[1]\";\n".
	"\tservice = \"$PROVIDER_PORT\";\n".
	"\tthreads = 100;\n".
	"\tpages = $PROVIDER_PAGE_NO;\n".
	"\tphost = \"$_[2]\";\n".
	"\tpservice = \"$PUBLISHER_PORT\";\n".
	"};\n";
    open(PROVIDER_OUT, ">$WORKING_DIR/$_[0]/provider_$_[1].cfg");
    print PROVIDER_OUT $template;
    close(PROVIDER_OUT);
}

sub gen_manager_template {
    my $template = "$_[0]: {\n".
	"\thost = \"$_[1]\";\n".
	"\tservice = \"$_[2]\";\n".
	"\tthreads = 100;\n".
	"};\n";
    return $template;    
}

sub gen_manager_config {
    my $template = gen_manager_template($_[1], $_[2], $_[3]);
    open(MANAGER_OUT, ">$WORKING_DIR/$_[0]/$_[1].cfg");
    print MANAGER_OUT $template;
    close(MANAGER_OUT);
};

sub gen_dht_config {
    my @hosts = @{$_[1]}, $no_bamboo = $_[2];
    my $template = "dht: {\n".
	"\tgateways = (\n\t\t".join(",\n\t\t", map("\"$_\"", @hosts[2..($no_bamboo - 1)]))."\n\t);\n".
	"\tport = \"$BAMBOO_RPC_PORT\";\n".
	"\tretry = 5;\n".
	"\ttimeout = 2;\n".
	"\tcachesize = 1048576;\n".
	"\tthreads = $no_hosts;\n".
	"};\n".
	gen_manager_template('publisher', $hosts[0], $PUBLISHER_PORT).
	gen_manager_template('lockmgr', $hosts[1], $LOCKMGR_PORT);
    open(TEST_OUT, ">$WORKING_DIR/test_$_[0].cfg");
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
    my $bamboo_hosts = $_[2];
    my $provider_hosts = $_[3];
    deploy_process($hosts[0], $PUBLISHER_RUN, "$WORKING_DIR/$job_id/publisher.cfg", "/tmp/$LOGIN_NAME/$job_id/publisher");
    deploy_process($hosts[1], $LOCKMGR_RUN, "$WORKING_DIR/$job_id/lockmgr.cfg", "/tmp/$LOGIN_NAME/$job_id/lockmgr");
    for ($i = 2; $i < $bamboo_hosts; $i++) {
	print "Now processing host $hosts[$i] ($i/$no_hosts)...";
	#deploy_process($hosts[$i], $BAMBOO_RUN, "$WORKING_DIR/$job_id/bamboo_$hosts[$i].cfg", "/tmp/$LOGIN_NAME/$job_id/bamboo");
	deploy_process($hosts[$i], $BAMBOO_RUN, "$WORKING_DIR/$job_id/provider_$hosts[$i].cfg", "/tmp/$LOGIN_NAME/$job_id/sdht");
    }
    for ($i = 2; $i < $provider_hosts; $i++) {
	print "Now processing host $hosts[$i] ($i/$no_hosts)...";
	deploy_process($hosts[$i], $PROVIDER_RUN, "$WORKING_DIR/$job_id/provider_$hosts[$i].cfg", "/tmp/$LOGIN_NAME/$job_id/provider");
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
	    "ps aux | grep publisher | grep -v grep;".
	    "ps aux | grep lock_mgr | grep -v grep;".
	    #"ps aux | grep bamboo.lss.DustDevil | grep -v grep";
	    "ps aux | grep shdt | grep -v grep";
	print `oarsh $hosts[$i] \"$pscmd\"`;
    }
}

#################################################  MAIN PROGRAM  ###############################################

# get params 
$usage = "Usage: noadage-deploy.pl -job <oar_job_id> [-kill | -status] [-bamboo n] [-providers n]";
$job_id = 0;
$bamboo_hosts = 0;
$provider_hosts = 0;
GetOptions('job=i' => \$job_id, 
           'kill' => \$kill_flag, 
	   'status' => \$check_flag,
	   'bamboo=i' => \$bamboo_hosts,
	   'providers=i' => \$provider_hosts,
	   'nodes=s' => \$nodes_file
	  ) || die $usage;
if ($job_id == 0) { die $usage; }
$ENV{'OAR_JOB_KEY_FILE'} = "$HOME_DIR/keys/oargrid_ssh_key_".$LOGIN_NAME."_$job_id";

# Get hosts
@hosts = get_hosts_file($nodes_file);
#@hosts = ("paramount1", "paramount2", "paramount3");
if (@hosts == 0) { die "Could not parse the reservation host list\nMake sure job ID is valid & you are running on frontend"; }
$no_hosts = @hosts;
printf "oar2 reports $no_hosts reserved nodes for $job_id\n";
if ($no_hosts < 3) { die "FATAL: Must reserve at least 3 nodes"; } 
if ($bamboo_hosts == 0) { $bamboo_hosts = $no_hosts - 2; }
if ($provider_hosts == 0) { $provider_hosts = $no_hosts - 2; }

if ($kill_flag) {
    kill_manually(\@hosts);
    exit(0);
}
if ($check_flag) {
    getstatus_manually(\@hosts);
    exit(0);
}

# read template files
$BAMBOO_TEMPLATE = read_template($BAMBOO_TEMPLATE_FILE);

# create directories 
$dir = $job_id;
rmtree([$dir], 0, 1);
mkdir($dir);

# 0 and 1 are the publisher & lock manager
gen_manager_config($job_id, "publisher", $hosts[0], $PUBLISHER_PORT);
gen_manager_config($job_id, "lockmgr", $hosts[1], $LOCKMGR_PORT);

# starting from 2 place bamboo nodes and providers on the same host
my @glist = ($hosts[2]);
$bamboo_hosts = $bamboo_hosts + 2;
$provider_hosts = $provider_hosts + 2;
gen_bamboo_config($job_id, $hosts[2], \@glist);
gen_provider_config($job_id, $hosts[2], $hosts[0]);
for ($i = 3; $i < $bamboo_hosts; $i++) {
    my $left = $i - 10; if ($left < 2) { $left = 2; }
    my @glist = @hosts[$left..$i - 1]; 
    gen_bamboo_config($job_id, $hosts[$i], \@glist);
}
for ($i = 3; $i < $provider_hosts; $i++) {
    gen_provider_config($job_id, $hosts[$i], $hosts[0]);
}
print "Generated configuration files successfully for $no_hosts nodes\n";

# Deployment
print "Starting deployment...\n";
deploy_manually($job_id, \@hosts, $bamboo_hosts, $provider_hosts);
print "Deployment suceessful\n";

# Postprocessing
print 'Postcleanup...', "\n";
rmtree([$dir], 0, 1);
gen_dht_config($job_id, \@hosts, $bamboo_hosts);

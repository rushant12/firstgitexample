# =============================================================================================
# $IBSrevision: 2.3.0 $DateTime: 2025/10/27 08:31:58 $
# =============================================================================================
# README: (Seriously, read it and understand it; it will save you many production headaches...)
#
# 1) Read all comments everywhere in the skeleton code at least once before you start changing 
#    anything; THEY EXIST FOR GOOD REASONS that YOU need to know (and keep them updated with 
#    accurate info please, describing "why", not "what"; the "what" of the code should be 
#    self-documented by choosing clear and precise variable and subroutine names -- do not 
#    hesitate to use a 40-character-long name if it's needed for clarity)!
#
# 2) When this system is first created, be sure to request that the SRE team mount the NFS 
#    share using the lookupcache=none mount option, and have them use NFS version 4 (as opposed 
#    to version 3 which is currently more common).  The latter allows file locking to actually 
#    work reliably, as intended, with any locking mechanism which is based on the kernel's 
#    flock implementation (which most are), and the former is due to the following:
#
#    Unfortunately, the NFS filesystems, by default, are mounted with attribute caching, in 
#    particular, acdirmin defaults to 30 seconds and acdirmax to 60 seconds see nfs manpage, 
#    starting with section 'Directory entry caching'. This means that, often, a process on one 
#    server will create a file and then a process on another server, less than 60 seconds 
#    later, will look to see if the file exists, but due to the directory attribute caching, 
#    the directory's mtime is stale; thus the client uses it's cached LOOKUP results and does 
#    not see the new file.
#    
#    Now, the code which looks for the file could simply fail (or wait and check again later),
#    but that's only possible if we know for sure that the file is supposed to exist in the 
#    first place; however, we often use the existence of a file or group of files as a trigger 
#    for other job steps. For instance, to instantiate repeatable groups, we often use a "glob" 
#    to create a list of files, each of which start their own repeatable group.  If one or more 
#    files is missing from the list because of this NFS caching, then the corresponding 
#    repeatable group is never triggered and the preprocessor proceeds along it's merry way 
#    none-the-wiser (this does actually happen periodically in current production systems, but 
#    most people never understand why and, instead, just restart the job and everything works 
#    that second time around; so they are doomed to repeat the process in the future).  The 
#    solution for this is to add the lookupcache=none mount option to the /etc/fstab of all the 
#    servers which mount our filesystem.
#
# 3) All exec_* and init_* subroutines should call standard_job_step_setup before doing anything 
#    else; post_* and fail_* subroutines should NOT call it.
#
# 4) Never use die directly to fail a job!  Instead use the fail subroutine, as it handles 
#    cleanup of any lock files and lets any resubmitted job step know that it was a resubmit.  
#    Any BEGIN blocks should be an obvious exception. update: there is a safeguard 
#    $SIG{__DIE__} handler in place now at the module level to prevent issues when people 
#    decide these comments are "TLDR"... But I'm leaving this comment because you still need to 
#    know this.
#    Also, every FM templated job step should have a fail_* routine defined and that routine 
#    should call handle_failure_without_exiting and then return success, rather than using 
#    fail.
#
# 5) Do not just "print" to stdout to log info.  Use the fm_sub.pm logging routines (log_info, 
#    log_warn, etc.) for that; otherwise you may cause broken lines in the log (lines that 
#    appear in the log without the pipe field-separators), which foils log parsers that expect 
#    the FM log file structure and may cause us to missing important info when troubleshooting 
#    as well.
#
# 6) Please always use the common_lib::set_env_var (or alternatively fm_sub::publishToJob) to
#    set environment variables rather than setting them directly; so that we get a consistent 
#    log entry every time a run parameter is tweaked.  Seeing the complete environment in the 
#    log files is very important for production troubleshooting! Thank you.
#
# 7) In order to facilitate testing of date/time-specific logic and to avoid certain race 
#    conditions, use common_lib::$SECONDS_SINCE_UNIX_EPOCH, instead of time(), where 
#    appropriate (see comments where $SECONDS_SINCE_UNIX_EPOCH is defined).
#
# 8) Last, but certainly not least, what I call the "golden rule":  In general throughout these 
#    scripts, for the sake of making an ESP job step "resubmit" work, and for being able to 
#    look at the whole pipe-line of WIP files when troubleshooting a production issue, please 
#    NEVER MODIFY or move any job step's INPUT files.  Instead, only read input and create NEW 
#    output.  Even if the job step does nothing but archival, archive a copy only, leaving the 
#    original in place, so the resubmit will still work (the obvious exception being the 
#    original input file(s) registered for the event, as they need to be moved to save/, 
#    but only after the rest of job is complete).
# =============================================================================================
use strict;
use warnings;

# =============================================================================================
# Declare our module's interface
# =============================================================================================
package fm_custom_lib;
require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    exec_extract_input_to_wip
    exec_collect_pdf_page_counts
    exec_convert_N_pdfs_to_1_afp
    exec_modifyafp
    init_afpgen
    fail_afpgen
    post_afpgen
    init_std2fact
    post_std2fact
    fail_std2fact
    init_send_afp_to_ur
    fail_send_afp_to_ur
    post_send_afp_to_ur
    init_submit_billing_info
    post_submit_billing_info
    fail_submit_billing_info
    init_update_eot
    fail_update_eot
    post_update_eot
    exec_general_cleanup
    exec_register_cleanup_event_if_needed
    exec_archive_input
	exec_create_reporting_solutions_side_file
	exec_send_reporting_solutions_side_file
);

# =============================================================================================
# Dependencies
# =============================================================================================
# perl core modules
use v5.10; # this is the version of perl FM is currently calling us with; so we need to stick to it's interface
use FindBin;
use File::Basename;
use File::Copy;
use File::Path qw(mkpath);
use File::stat;
use Cwd qw(getcwd chdir); # overriding built in chdir so that $ENV{'PWD'} stays updated
use Time::Local;
use Data::Dumper;
use JSON::PP;
use POSIX qw(tzset);

# Prepare the environment for loading of the non-core modules:
BEGIN {
    # Ensure we have required paths
    if (! defined $ENV{'BASEPATH'}) {
        # We assume that the main script is in our system bin dir
        # (we're running a cron job or a CUSTOM_ACTION via file_transfer.pl),
        # since the only other option is that FM is calling us, in which
        # case BASEPATH would already be populated.
        my $base_path = $FindBin::Bin;
        $base_path =~ s!/system/bin!!;
        $ENV{'BASEPATH'} = $base_path;
        print "BASEPATH was not defined; it is now set to: $ENV{'BASEPATH'}\n";
    }
    if (! defined $ENV{'STAGE'}) {
        if ($ENV{'BASEPATH'} =~ m!/dsto/cbu/(dev|qa|int|uat|prod)/!) {
            $ENV{'STAGE'} = $1;
        }
        else {
            die "Unable to derive STAGE based on the BASEPATH, which is: $ENV{'BASEPATH'}";
        }
        print "STAGE was not defined; it is now set to: $ENV{'STAGE'}\n";
    }
    if (! defined $ENV{'FM_BASEPATH'}) {
        $ENV{'FM_BASEPATH'} = "/dsto/sw/$ENV{'STAGE'}/fm";
        print "FM_BASEPATH was not defined; it is now set to: $ENV{'FM_BASEPATH'}\n";
    }
}

# platform modules
use lib "$ENV{'FM_BASEPATH'}/system/bin";
use fm_sub;

# system modules
use lib "$ENV{'BASEPATH'}/system/bin";
use fm_custom_hook qw(
    export_hook_cfg_info
);
use corp_map qw(
    get_corp_map
);
use common_lib qw(
    $GLOBAL_LOCK_DIR
    $SECONDS_SINCE_UNIX_EPOCH
    load_to_env
    running_in_DR_mode
    set_env_var
    obtain_job_lock
    release_job_lock
    reobtain_job_lock_if_needed
    restage_input_files_if_needed
    archive
    exec_shell_command_or_fail
    exec_bash_command2
    check_for_basic_needed_env_vars
    handle_failure_without_exiting
    ensure_dir_exists
    fail
    replace_vars_in_file
    move_file
    get_files_from_afpgen_stats
    archive_copy_of
    get_system_timezone
	exec_file_transfer_script
);
# Note: we also want common_lib's intialization (Stuff that every preprocessor should do; 
# overriding CLARIFY_TEST_MODE, umask, etc.)

# =============================================================================================
# Module initialization
# =============================================================================================
# Run all "exceptions" through our fail function, unless it was thrown inside an eval block, in 
# which case allow the evaling code to handle it by dying with the original message.
# (see https://perldoc.perl.org/functions/die for details)
#local $SIG{'__DIE__'} = sub { die @_ if $^S; &fail("$_[0]"); };
# ugg; making the handler local doesn't seem to work at the module level?  We work around it by 
# saving the old handler and restoring it just before failing:
my $prev_die_handler = $SIG{__DIE__};
$SIG{__DIE__} = sub {
    $SIG{__DIE__} = $prev_die_handler;
    #die @_ if $^S;
    # Actually, can't do the above $^S check since FM is evaling our subs as well (so it's 
    # always set to true); will just have to ensure that all of our eval blocks set "local 
    # $ENV{'__DIE__'}" instead.
    &fail("$_[0]");
};

# =============================================================================================
# Public Data (Any data intended to be shared with clients importing our module.  Note the 
# "our" instead of "my" and remember that these values should also be added to @EXPORT_OK.)
# =============================================================================================

# =============================================================================================
# Private Data (file-scoped and set by standard_job_step_setup when not initialized here)
# =============================================================================================
my @INPUT_FILES = (); # all files registered to this FM event
my $HOLD_DIR;
my $TRUE = 1;
my $FALSE = 0;
my $ENSURE_INPUT_FILES_EXIST = $TRUE;
my $NUM_SECONDS_PER_MINUTE = 60;
my $NUM_SECONDS_PER_HOUR   = 60 * 60;
my $NUM_SECONDS_PER_DAY    = 60 * 60 * 24;
my $GENERAL_CLEANUP_LOCK_BASE_NAME = "general_cleanup.lock";
my $CORP_MAP_REF;
my $SEND_PRINT_FILE;
my $DO_BILLING;
my $SEND_CSR_REPORTS;
my $SEND_TO_UR_LOADER;
my $PRODUCT_SPECIFIC_CONFIG_DIR;

# =============================================================================================
# public subroutines
# =============================================================================================
sub exec_extract_input_to_wip
{
    &standard_job_step_setup($ENSURE_INPUT_FILES_EXIST);
        # This is the first job step; so it needs the registered input files to work; hence the 
        # true-valued flag passed here.

    my $input_file = "$INPUT_FILES[0]";
    my $extracted_dir = "$ENV{'WIP_DIR'}/pdf_input";
    `rm -rf "$ENV{'WIP_DIR'}/locks"`;
    `rm -rf "$ENV{'WIP_DIR'}/pdf_input"`;
    &ensure_dir_exists("$extracted_dir");

    my $cmd = "cd $extracted_dir && /usr/bin/tar -xzvf $input_file"; # $CWD is WIP_DIR
    &exec_shell_command_or_fail(
        'cmd'        => "$cmd",
        'log_output' => $FALSE,
        'prefix_msg' => "Failed to extract input file",
    );

    &fm_sub::publishToJob("EXTRACTED_INPUT_DIR", "$extracted_dir");

    my $base_name = &basename("$input_file");
    $base_name =~ s/\.tgz$//;
    my ($metadata_file) = glob "$ENV{EXTRACTED_INPUT_DIR}/${base_name}.json";
    &fail("Expected metadata file, ${base_name}.json, does not exist in input file, $input_file") unless -e "$metadata_file";
    &fm_sub::publishToJob("INPUT_METADATA_FILE", "$metadata_file");

    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub exec_collect_pdf_page_counts
{
    &standard_job_step_setup();

    # convert metadata JSON file to a perl data structure
    my ($orig_metadata_file) = "$ENV{'INPUT_METADATA_FILE'}";
    my $json_input_text = &slurp("$orig_metadata_file");
    my $json = JSON::PP->new->utf8();
    my $metadata_href = $json->decode("$json_input_text");
    #say Dumper($metadata_href);

    my $pdfs_dir = "$ENV{'EXTRACTED_INPUT_DIR'}";
    my $found_errors = $FALSE;
    foreach my $mail_piece_href (@{$metadata_href->{'MAIL_PIECES'}}) {
        my $total_page_count = 0;
        my $total_sheet_count = 0;
        foreach my $pdf_href (@{$mail_piece_href->{'pdf_documents'}}) {
            # temporary kludge to create a test data set:
            #unless (-e "${pdfs_dir}/$pdf_href->{'file_name'}") {
            #    &copy("$ENV{'WIP_DIR'}/blah.pdf", "${pdfs_dir}/$pdf_href->{'file_name'}") or &fail("Unable to copy $ENV{'WIP_DIR'}/blah.pdf to ${pdfs_dir}/$pdf_href->{'file_name'}");
            #}
            my $result = &exec_bash_command2("$ENV{'BASEPATH'}/system/bin/pdfinfo ${pdfs_dir}/$pdf_href->{'file_name'}");
            #say Dumper($result);
            #if ($result->{'rc'} != 0 or "$result->{stderr}") {
            if ($result->{'rc'} != 0) {
                &fm_sub::log_error("Failed to find page count of $pdf_href->{'file_name'}: pdfinfo found the following error (jobstep will fail after all pdfs are scanned):\n$result->{stderr}");
                $found_errors = $TRUE;
                next;
            }
            my ($page_count) = grep {/^Pages:/} split /\n/, "$result->{stdout}";
            $page_count =~ s/^Pages:\s+(\d+)/$1/;
            $pdf_href->{'page_count'} = "$page_count";
            my $sheet_count = int (($page_count + 1) / 2);
            $total_page_count += $page_count;
            $total_sheet_count += $sheet_count;
        }
        $mail_piece_href->{'total_pdf_documents_page_count'} = $total_page_count;
        $mail_piece_href->{'total_pdf_documents_sheet_count'} = $total_sheet_count;
    }
    &fail("Found errors in one or more PDFs; see the job log for details.") if $found_errors;
    #TODO: Can pdfinfo read multiple files at the same time, to improve performance (if so, use 
    #xargs and collect all stdouts before updating json object; in fact just collect page 
    #counts into a hash before even looping through json)?

    # output an updated metadata file (following the golden rule here; see comments at top of 
    # this file)
    $json = $json->pretty(["true"]);
    my $json_output_text = $json->encode($metadata_href);
    my $updated_metadata_file = "$ENV{'WIP_DIR'}/mailpiece_metadata.json";
    open(my $file, '>', "$updated_metadata_file") or &fail("Unable to open '$updated_metadata_file': $!");
    print $file "$json_output_text\n" or &fail("Unable to write to '$updated_metadata_file': $!");
    close $file or &fail("Unable to close '$updated_metadata_file': $!");
    &fm_sub::publishToJob("MAIL_PIECE_METADATA_FILE", "$updated_metadata_file");

    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub exec_convert_N_pdfs_to_1_afp
{
    &standard_job_step_setup();

    my $pdfs_dir = "$ENV{'EXTRACTED_INPUT_DIR'}";
    my $output_file = "$ENV{'WIP_DIR'}/$ENV{'CORP'}.$ENV{'RUNDATE'}.$ENV{'CYCLE'}.pdf2afp_out.afp";
        # named with corp/rundate/cycle for easy searching of WIP_DIR for prod support.


    # Set environment needed to run
    my $clobber = "false"; # allows environment (ops.rc, fmo.rc, FM setup) to override our defaults
    &load_to_env("$ENV{'BASEPATH'}/system/config/pdf2afp.cfg", $clobber);
    my $compart_version = $CORP_MAP_REF->{'compart_version'};
    &set_env_var('JAVA_HOME', "/dsto/shared/java/java11_64");
    my $dbmill_toolkit_basepath ="/dsto/sw/$ENV{'STAGE'}/dbmill/DBMILL_TOOLKIT_V_${compart_version}";
    &set_env_var('CPLICENSEPATH', "${dbmill_toolkit_basepath}/system/config/");
    &set_env_var('CPTEMP', "$ENV{'WIP_DIR'}");
    #my $env_properties_file = "$ENV{WIP_DIR}/$ENV{'CORP'}.$ENV{'RUNDATE'}.$ENV{'CYCLE'}.pdf2afp_env.txt";
    #&dump_environment_to("$env_properties_file");
            #. " -DENV_VARS_FILE=$env_properties_file"

    my $starting_memory_allocation = "$ENV{'BEGIN_MEMORY'}";
    my $max_memory_allocation = "$ENV{'MAX_MEMORY'}";
    my $cmd = "$ENV{'JAVA_HOME'}/bin/java"
            . " -Xms${starting_memory_allocation} -Xmx${max_memory_allocation}"
            . " -cp ${dbmill_toolkit_basepath}/system/bin/cpjdlib.jar:$ENV{'BASEPATH'}/system/bin/pdf2afp-$CORP_MAP_REF->{'pdf2afp_version'}.jar"
            . " -Djava.library.path=${dbmill_toolkit_basepath}/system/lib"
            . " com.broadridge.pdf2print.dataprep.pdf2afp.Main"
            . " -config_file ${PRODUCT_SPECIFIC_CONFIG_DIR}/pdf2afp.json"
            . " -input_dir $ENV{'WIP_DIR'}/pdf_input"
            . " -profile_dir ${PRODUCT_SPECIFIC_CONFIG_DIR}"
            . " -metadata_file $ENV{'MAIL_PIECE_METADATA_FILE'}"
            . " -trace_file $ENV{'WIP_DIR'}/dbmill.trc"
            . " -output_file $output_file"
            ;
    $cmd .= " -debug" if defined $ENV{'PDF2AFP_DEBUG'} and uc($ENV{'PDF2AFP_DEBUG'}) eq "YES";
    &exec_shell_command_or_fail(
        'cmd'        => "$cmd",
        'prefix_msg' => "Failed to convert PDFs to AFP",
    );

    &fm_sub::publishToJob("PDF2AFP_OUTPUT_FILE", "$output_file");

    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub exec_modifyafp
{
    &standard_job_step_setup();

    my $input_file = "$ENV{'PDF2AFP_OUTPUT_FILE'}";
    my $output_file = "$ENV{'WIP_DIR'}/$ENV{'CORP'}.$ENV{'RUNDATE'}.$ENV{'CYCLE'}.modifyafp_out.afp";

    # Set environment needed to run jolt
    &set_env_var('JOLT_MSG_FILE', "$ENV{'BASEPATH'}/system/config/modifyafp_messages.cfg");
    &set_env_var('JOLT_STMT_REPORT_FILE', "$ENV{'WIP_DIR'}/jolt_stmt_report.txt");
    &set_env_var('JOLT_ORIG_INPUT_FILE', "");

    # Set environment needed to run modifyafp
	my $clobber = "false"; # allows environment (ops.rc, fmo.rc, FM setup) to override our defaults
	&load_to_env("$ENV{'BASEPATH'}/system/config/modifyafp.cfg", $clobber);
    &set_env_var('JAVA_HOME', "/dsto/shared/java/java11_64");
    &set_env_var('IS_BULK_SHIPPING', $CORP_MAP_REF->{'is_bulk_shipping'});
    my $env_properties_file = "$ENV{WIP_DIR}/$ENV{'CORP'}.$ENV{'RUNDATE'}.$ENV{'CYCLE'}.modifyafp_env.txt";
    &dump_environment_to("$env_properties_file");

    my $starting_memory_allocation = "$ENV{'BEGIN_MEMORY'}";
    my $max_memory_allocation = "$ENV{'MAX_MEMORY'}";
    my $cmd = "$ENV{'JAVA_HOME'}/bin/java"
            . " -Xms${starting_memory_allocation} -Xmx${max_memory_allocation}"
            . " -DENV_VARS_FILE=$env_properties_file"
            . " -jar $ENV{'BASEPATH'}/system/bin/modifyafp-$CORP_MAP_REF->{'modifyafp_version'}.jar"
            . " -configFile ${PRODUCT_SPECIFIC_CONFIG_DIR}/modifyafp.json"
            . " -mailPieceMetadataFile $ENV{'MAIL_PIECE_METADATA_FILE'}"
            . " -corp $ENV{'CORP'}"
            . " -rundate $ENV{'RUNDATE'}"
            . " -cycle $ENV{'CYCLE'}"
            . " -if $input_file"
            . " -of $output_file"
            ;
    $cmd .= " -debug" if defined $ENV{'MODIFYAFP_DEBUG'} and uc($ENV{'MODIFYAFP_DEBUG'}) eq "YES";
    &exec_shell_command_or_fail(
        'cmd'        => "$cmd",
        'prefix_msg' => "Failed to apply business rules to AFP file",
    );

    &fm_sub::publishToJob("MODIFYAFP_OUTPUT_FILE", "$output_file");

    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub init_afpgen
{
    &standard_job_step_setup();

    my $master_config_template = "${PRODUCT_SPECIFIC_CONFIG_DIR}/afpgen.cfg";
    my $master_config = &prepare_master_config("$master_config_template", "$ENV{'WIP_DIR'}");
    &set_env_var('MASTER_ELEMENT_CONFIG', $master_config);
    &set_env_var('AFPGEN_IF', $ENV{'MODIFYAFP_OUTPUT_FILE'});
    &set_env_var('AFPGEN_HOME', "/dsto/sw/$ENV{'STAGE'}/afpgen");
    &set_env_var('AFPFONTPATH', "/dsto/natresr/$ENV{'STAGE'}/vip/font/cfl_4.0/afnt");
    &set_env_var('AFPGEN_STATISTICS_DIR', $ENV{'WIP_DIR'});
    &set_env_var('AFPGEN_OF', "$ENV{'WIP_DIR'}/$ENV{'CORP'}.$ENV{'RUNDATE'}.$ENV{'CYCLE'}.afpgen_output.afpds");

    return $fm_sub::SUCCESS;
}

sub post_afpgen
{
    #if ("$ENV{'event_type_id'}" eq "$ENV{'PROCESS_DAT_FILE'}") { # for UR files, as opposed to infact files
    #    # We don't need the product-specific streams; only need the UR stream; so remove the 
    #    # other afpgen output files to save disk space (I tried to set the output files to 
    #    # /dev/null instead, but that didn't work).
    #    foreach my $stream ("PS", "QV", "EVSB") {
    #        foreach my $file (&get_files_from_afpgen_stats("$stream")) {
    #            if (defined $file and -e "$file") {
    #                unlink "$file" or &fail("Unable to remove '$file': $!");
    #            }
    #        }
    #    }
    # }

     &fm_sub::log_info("Job Step Completed Successfully\n");
     return $fm_sub::SUCCESS;
}

sub fail_afpgen
{
    &handle_failure_without_exiting("afpgen failed; see log for details");

    # FM still fails even though we're returning success here.  We're saying that our failure 
    # handling was successful, not the initial job step...
    return $fm_sub::SUCCESS;
}

sub init_std2fact
{
    &standard_job_step_setup();

    my ($ps_file) = &get_files_from_afpgen_stats("PS");
    &set_env_var('STD2FACT_IF', "$ps_file");
    if (not $SEND_PRINT_FILE) {
        &fm_sub::log_info("Skipping Std2Fact...\n");
        &archive("$ENV{'STD2FACT_IF'}", "$ENV{'OUTPUT_DIR'}/to_fact/save");
        return $fm_sub::JOB_STEP_BYPASS;
    }

    # Set print facility.
    # possible values for STD2FACT_POOL:
    # INFACT_HAR
    # INFACT_KC
    # INFACT_EDH
    # INFACT_CPL
    # INFACT_CPL_US_ONLY
    # INFACT_EDG
    my $infact_site = uc($CORP_MAP_REF->{'infact_site'});
    if ("$ENV{'STAGE'}" eq "uat") {
        &set_env_var('STD2FACT_POOL', "INFACT_HAR");
    }
    else {
        &set_env_var('STD2FACT_POOL', "INFACT_${infact_site}");

    }

    # Set target directory
	if (not defined $ENV{'DISKFARM_COMMON_DIR'}) { # overridable in fmo.rc or ops.rc
        #if ("$ENV{'STAGE'}" eq "uat") {
        #    &set_env_var('DISKFARM_COMMON_DIR', "/dsto/factory/$ENV{'STAGE'}/har/infact/input");
        #}
        #elsif ("$infact_site" eq "EDH") {
        #    &set_env_var('DISKFARM_COMMON_DIR', "/dsto/sw/$ENV{'STAGE'}/Std2Fact/input");
        #}
        #else {
        #    &set_env_var('DISKFARM_COMMON_DIR', "/dsto/factory/$ENV{'STAGE'}/".lc($infact_site)."/infact/input");
        #}
        # ... actually, the enivornments are not consistent; so these dirs don't exist as 
        # expected on all servers in pool in all stages.  Trying alternate solution, writing to 
        # our wip dir; apparently it doesn't matter where the file lands, as long as the infact 
        # router job (started behind the scenes by the FM code run by this templated job step) 
        # can read the file, meaning the server must have our NFS share mounted) can read the 
        # file, (meaning the server must have our NFS share mounted).  The router job 
        # apparently runs in an infact pool and since that has nothing to do with us, it should 
        # not be our job to ensure the servers on that pool have our share mounted, but I can't 
        # get platform support to take accountability; so we will have to find out if any 
        # servers need our filesystem mounted and request it ourselves...
        &set_env_var('DISKFARM_COMMON_DIR', "$ENV{'BASEPATH'}/wip");
    }

    # Some non-default settings that need to be set for linux (AIX did not 
    # require them):
    &set_env_var('STD2FACT_CORP', $ENV{'CORP'});
    &set_env_var('STD2FACT_RD', $ENV{'RUNDATE'});
    &set_env_var('STD2FACT_CYC', $ENV{'CYCLE'});
    # Do not set this one in the fm setup, else, STD2FACT_POOL will be eval'd 
    # before we populate it.
    &set_env_var('STD2FACT_INFACT_ROUTER', "-startESP -infactPool $ENV{'STD2FACT_POOL'}");

    # Override default output filename to include the system name in case we need to find all 
    # output files from a system and don't know all the corp numbers. Note that the FM std2fact 
    # script also adds a timestamp to the end of this file name.
    &set_env_var('STD2FACT_OF', "$ENV{'DISKFARM_COMMON_DIR'}/$ENV{'CORP'}/".lc($ENV{'FM_SYSTEM_NAME'}).".$ENV{'CORP'}.$ENV{'RUNDATE'}.$ENV{'CYCLE'}.afpds");

    return $fm_sub::SUCCESS;
}

sub fail_std2fact
{
    &handle_failure_without_exiting("Std2fact failed; see log for details");

    # FM still fails even though we're returning success here.  We're saying that our failure 
    # handling was successful, not the initial job step...
    return $fm_sub::SUCCESS;
}

sub post_std2fact
{
    &archive_copy_of($ENV{'STD2FACT_IF'}, "$ENV{'BASEPATH'}/output/to_fact/save");

    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub init_send_afp_to_ur
{
    &standard_job_step_setup($ENSURE_INPUT_FILES_EXIST);

    if ($ENV{'event_type_id'} eq $ENV{'RETRANSFER_TO_UR_LOADER_EVENT_ID'}) {
        &set_env_var('URLDR_INPUT_FILE', "$INPUT_FILES[0]");
    }
    else {
        my ($ur_file) = &get_files_from_afpgen_stats("UR");
        &set_env_var('URLDR_INPUT_FILE', "$ur_file");

        # Optionally suppress UR load here, rather than in the parent block so that manual 
        # loads can still be done with the resend event:
        if (not $SEND_TO_UR_LOADER) {
            &fm_sub::log_info("Skipping AFP2UR load...\n");
            &archive("$ENV{'URLDR_INPUT_FILE'}", "$ENV{'OUTPUT_DIR'}/to_internal/save");
            return $fm_sub::JOB_STEP_BYPASS;
        }
    }
    &set_env_var('URLDR_SRC', "IF");
    if ( $ENV{'STAGE'} eq "UAT" or uc($ENV{'FORCE_UAT_UR'}) eq "TRUE" ) {
        &fm_sub::log_info("Sending to UAT loader instead of prod...");
        &set_env_var('URLDR_USE_AUX', "true");
    }

    # override all FM-templated destinations for new cloud loaders:
    &set_env_var('URLDR_DEVQA_SNODE', 'CDQAAWSBPSDASH');
    &set_env_var('URLDR_DEVQA_SNODEID', '');
    &set_env_var('URLDR_UAT_SNODE', 'CDUATAWSBPSDASH');
    &set_env_var('URLDR_UAT_SNODEID', '');
    &set_env_var('URLDR_PROD_SNODE', 'CDPRDAWSPCINDM');
    &set_env_var('URLDR_PROD_SNODEID', '');

    return $fm_sub::SUCCESS;
}

sub fail_send_afp_to_ur
{
    &handle_failure_without_exiting("Transfer of AFP to the UR failed; see log for details");

    # FM still fails even though we're returning success here.  We're saying that our failure 
    # handling was successful, not the initial job step...
    return $fm_sub::SUCCESS;
}

sub post_send_afp_to_ur
{
    &archive("$ENV{'URLDR_INPUT_FILE'}", "$ENV{'BASEPATH'}/output/to_internal/save");

    return $fm_sub::SUCCESS;
}

sub init_submit_billing_info
    # Note that this billing step only needs to be called once, after N calls
    # to fm_sub::submitBillingPackage are made in previous steps. The billing
    # step (which this sub initializes) will then call the billing API for each
    # billing package submitted to FM. If doing corp splitting, for instance,
    # the normal useage is for each corp's repeatable group to call
    # fm_sub::submitBillingPackage to bill for that corp, but this billing step
    # itself should only be called once after the groups are done.
    # It just so happens that for our bbdfmstmt usage, it's convenient for us to do all the
    # fm_sub::submitBillingPackage calls here in the init subroutine.
{
    &standard_job_step_setup();

    &fm_sub::log_info("Initializing billing...\n");

    my $is_single_billing_rerun = ($ENV{'event_type_id'} eq $ENV{'BILLING_RERUN_EVENT_ID'});
    my $corp_map_do_billing = (uc($CORP_MAP_REF->{'do_billing'}) eq "Y");
    my $environment_do_billing = $DO_BILLING;
    if ( !$is_single_billing_rerun and (!$corp_map_do_billing or !$environment_do_billing) ) {
        &fm_sub::log_info("Skipping billing...\n");
        foreach my $file (&get_billing_api_files()) {
            &archive($file, "$ENV{'TO_INTERNAL_DIR'}/save");
        }
        return $fm_sub::JOB_STEP_BYPASS; # post_billing won't run
    }

    # In case of a "resubmit" of this jobstep, remove prior billing files
    # created by fm (this is the only jobstep that should be doing
    # submitBillingPackage calls; so this is safe).
    my @fm_billing_api_files = glob "$ENV{'FM_PARAM_DIR'}/fm_billing_api*";
    if (scalar(@fm_billing_api_files) > 0) {
        &fm_sub::log_info("Resubmit detected. Removing prior billing files\n");
        foreach my $file (@fm_billing_api_files) {
            &fm_sub::log_info("  Removing $file\n");
            unlink $file or &fail("Unable to remove $file.");
        }
    }

    my @files = ($is_single_billing_rerun) ? $INPUT_FILES[0] : &get_billing_api_files();
    foreach my $file (@files) {
        &fm_sub::log_info("Preparing to submit billing package for billing file: $file\n");
        my %billing_info = &get_billing_info($file);
        $billing_info{'CORP'} = $ENV{'BILLING_CORP'} if (defined $ENV{'BILLING_CORP'} and $ENV{'BILLING_CORP'});
        &fm_sub::submitBillingPackage(%billing_info);
    }

    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub fail_submit_billing_info
{
    &handle_failure_without_exiting("Failed to submit billing info; see log for details");

    # FM still fails even though we're returning success here.  We're saying that our failure 
    # handling was successful, not the initial job step...
    return $fm_sub::SUCCESS;
}

sub init_update_eot
{
    &standard_job_step_setup($ENSURE_INPUT_FILES_EXIST);

    if (defined $ENV{'SKIP_UPDATE_EOT'} && uc($ENV{'SKIP_UPDATE_EOT'}) eq "YES") {
        &fm_sub::log_info("Skipping UpdateEOT...\n");
        return $fm_sub::JOB_STEP_BYPASS;
    }

    # Set print facility.
    # possible values for STD2FACT_POOL:
    # INFACT_HAR
    # INFACT_KC
    # INFACT_EDH
    # INFACT_CPL
    # INFACT_CPL_US_ONLY
    # INFACT_EDG
    if ("$ENV{'STAGE'}" eq "uat") {
        &set_env_var('EOT_FACT', "INFACT_HAR");
    }
    else {
        &set_env_var('EOT_FACT', "INFACT_" . $CORP_MAP_REF->{'infact_site'});

    }

    # Convert Start Of Processing (the aggregator's start) YYYYMMDDhhmmss to seconds since Unix epoch:
    my $YYYY = substr("$ENV{'SOP_DATE_TIME_STAMP'}",0,4);
    my $MM   = substr("$ENV{'SOP_DATE_TIME_STAMP'}",4,2);
    my $DD   = substr("$ENV{'SOP_DATE_TIME_STAMP'}",6,2);
    my $hh   = substr("$ENV{'SOP_DATE_TIME_STAMP'}",8,2);
    my $mm   = substr("$ENV{'SOP_DATE_TIME_STAMP'}",10,2);
    my $ss   = substr("$ENV{'SOP_DATE_TIME_STAMP'}",12,2);
    my $epoch_sop_date_time_stamp = &common_lib::get_seconds_since_unix_epoch("${ss},${mm},${hh},${DD},${MM},${YYYY}");

    # Requirement is to set SOC 1 hr before aggregator starts:
    $epoch_sop_date_time_stamp -= $NUM_SECONDS_PER_HOUR;
    my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime($epoch_sop_date_time_stamp);
    &set_env_var('EOT_EOT', sprintf("%04d%02d%02d%02d%02d%02d", $year + 1900, $mon+1, $mday, $hour, $min, $sec));

    # Aggregator runs in EDH/Pacific timezone; hence SOP_DATE_TIME_STAMP is also Pacific.
    &set_env_var('EOT_TZ', "PST8PDT");
}

sub post_update_eot
{
     &fm_sub::log_info("Job Step Completed Successfully\n");
     return $fm_sub::SUCCESS;
}

sub fail_update_eot
{
    &handle_failure_without_exiting("UpdateEOT failed; see log for details");

    # FM still fails even though we're returning success here.  We're saying that our failure 
    # handling was successful, not the initial job step...
    return $fm_sub::SUCCESS;
}

sub post_submit_billing_info
{
    # TODO: rerun event does not yet exist
    my $is_single_billing_rerun = ($ENV{'event_type_id'} eq $ENV{'BILLING_RERUN_EVENT_ID'});
    unless ($is_single_billing_rerun) { # single billing event has it's own archive step
        &fm_sub::log_info("Archiving billing for CSRWeb...\n"); # TODO: is this for CSRweb only or also for RM?
        foreach my $file (&get_billing_api_files()) {
            &archive($file, "$ENV{'TO_INTERNAL_DIR'}/save");
        }
    }

    &fm_sub::log_info("Job Step Completed Successfully\n");

    return $fm_sub::SUCCESS;
}

sub exec_general_cleanup
    # This jobstep is meant for cleaning up old files which are purposefully left behind by 
    # previous runs (wip files, logs).  Please do not put cleanup logic here for "output" files 
    # generated during the run (reports, print files, etc).  Those should be cleaned up by the 
    # last job step which used the file(s) so that we don't have to have knowledge of every 
    # single file produced by every type of run, passed along to this jobstep.
    #
    # To avoid running cleanup when unnecessary (historically devs just insert this job step at 
    # the end of all their events, which caused it to run multiple times per day and often 
    # to bog down the systems and even cause a race condition between the multiple 
    # instances potentially running in parallel), this step should be called as an FM event 
    # using a "time-based" group (this is the standard). which is scheduled to run once per 
    # day.
    #
    # Also, note that I've specifically chosen not to use the common FM cleanup job step, 
    # because it's had many issues and continues to have issues with the backend server 
    # process, and the whole thing is really overcomplicated now (for historical reasons they 
    # rewrote the script as a wrapper to prevent multiple preprocessors from running it 
    # several times a day without having to deploy chode changes for all preprocessors).  
    # This new process is simple and makes use of the universally known unix find utility.
{
    &standard_job_step_setup($ENSURE_INPUT_FILES_EXIST);

    if (defined $ENV{'DO_GENERAL_CLEANUP'} && uc($ENV{'DO_GENERAL_CLEANUP'}) eq "NO") {
        &fm_sub::log_info("Bypassing cleanup due to DO_GENERAL_CLEANUP environment variable setting...\n");
        return $fm_sub::SUCCESS;
    }

    # ensure we don't multiple processes trying to clean up the same files at the same time:
    &obtain_job_lock('lock_name' => "$GENERAL_CLEANUP_LOCK_BASE_NAME");

    chdir "$ENV{'BASEPATH'}" or &fail("Unable to change dir to $ENV{'BASEPATH'}: $!");

    # Logs:
    # Compress after one week, delete after 2 months.
    &exec_shell_command_or_fail(
        'cmd'        => "/usr/bin/find log -ignore_readdir_race -type f -mtime +7 -exec gzip '{}' \\;",
        'log_output' => $FALSE,
        'prefix_msg' => "Unable to compress old log files",
    );
    &exec_shell_command_or_fail(
        'cmd'        => "/usr/bin/find log -ignore_readdir_race -type f -mtime +60 -delete",
        'log_output' => $FALSE,
        'prefix_msg' => "Unable to compress old log files",
    );

    # WIP files:
    # Compress after one week, delete after 2, remove any remaining empty dirs.
    # (keep in mind gzip preserves timestamps)
    &exec_shell_command_or_fail(
        'cmd'        => "/usr/bin/find wip -ignore_readdir_race -type f -mtime +7 -exec gzip '{}' \\;",
        'log_output' => $FALSE,
        'prefix_msg' => "Unable to compress old WIP files",
    );
    &exec_shell_command_or_fail(
        'cmd'        => "/usr/bin/find wip -ignore_readdir_race -type f -mtime +14 -delete",
        'log_output' => $FALSE,
        'prefix_msg' => "Unable to cleanup old WIP files",
    );
    &exec_shell_command_or_fail(
        'cmd'        => "/usr/bin/find wip ! -path wip/locks -mindepth 1 -depth -ignore_readdir_race -type d -empty -delete",
        'log_output' => $FALSE,
        'prefix_msg' => "Unable to cleanup empty WIP dirs",
    );

    # Archived Input and Output files:
    # (We assume these have been backed up to long-term storage by now by operations, a process 
    # exerternal to our code entirely.  They're already gzipped; so just delete them after 
    # a month.)
    &exec_shell_command_or_fail(
        'cmd'        => "/usr/bin/find input/save output/save output/to_internal/save output/to_cust/save output/to_fact/save -ignore_readdir_race -type f -mtime +30 -delete",
        'log_output' => $FALSE,
        'prefix_msg' => "Unable to cleanup old archived I/O files",
    );

    # Abandoned input/hold/*.dup files, left behind by NDM automatic retransfer attempts:
    # (In general, failures should be cleaned up manually, but this is a
    # constant annoyance, operations leaves behind these dup files. Just delete these after a 
    # month)
    &exec_shell_command_or_fail(
        'cmd'        => "/usr/bin/find input/hold -path 'input/hold/*.dup.*' -ignore_readdir_race -type f -mtime +14 -delete",
        'log_output' => $FALSE,
        'prefix_msg' => "Unable to cleanup old WIP files",
    );

    &release_job_lock();

    # ... and finally cleanup our cleanup event trigger
    my $file  = $INPUT_FILES[0];
    unlink "$file" or &fail("Unable to remove '$file': $!");

    return $fm_sub::SUCCESS;
}

sub exec_register_cleanup_event_if_needed
{
    &standard_job_step_setup();

    if (defined $ENV{'DO_GENERAL_CLEANUP'} && uc($ENV{'DO_GENERAL_CLEANUP'}) eq "NO") {
        &fm_sub::log_info("Bypassing cleanup trigger due to DO_GENERAL_CLEANUP environment variable setting...\n");
        return $fm_sub::SUCCESS;
    }

    # prevent other processes from competing with us
    &obtain_job_lock('lock_name' => "general_cleanup.lock");

    # Cleanup is a time-based event; so it's not triggering the actual job yet, but it's 
    # triggering the clock to start by ensuring at least one file is registered to the group.
    my $buffer_minutes_allowed = 30;
    my $suffix = get_next_cleanup_rundate("$ENV{'DEFAULT_FM_FORMAT_ID'}", "$ENV{DAILY_GENERAL_CLEANUP_EVENT_ID}", $buffer_minutes_allowed);
    my $trigger_file = "$ENV{'BASEPATH'}/input/hold/daily_cleanup_trigger_${suffix}";


    if (-e "$trigger_file") {
        &fm_sub::log_info("$trigger_file already exists; so presumably another process already registered it.  Skipping registration...");
    }
    else  {
        &exec_shell_command_or_fail(
            'cmd'        => "/usr/bin/touch '$trigger_file'",
            'log_output' => $FALSE,
            'prefix_msg' => "Unable to touch cleanup trigger file."
        );
        &fm_sub::register_files("$trigger_file")
            and &fail("Unable to register $trigger_file with FM.");
    }

    &release_job_lock();

    return $fm_sub::SUCCESS;
}

sub exec_archive_input
    # Note, that if you have any job steps after this one (which you really should try to avoid 
    # in your design), then you must pass a false value to &standard_job_step_setup() for all 
    # those steps, to avoid failure during the input file check.
{
    &standard_job_step_setup($ENSURE_INPUT_FILES_EXIST);

    foreach my $file (@INPUT_FILES) {
        &archive($file, "$ENV{'BASEPATH'}/input/save/");
    }

    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub exec_create_reporting_solutions_side_file()
{
    &standard_job_step_setup();
    my ($orig_metadata_file) = "$ENV{'MAIL_PIECE_METADATA_FILE'}";
    my $json_input_text = &slurp("$orig_metadata_file");
    my $json = JSON::PP->new->utf8();
    my $metadata_href = $json->decode("$json_input_text");
    my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime(time);
    my $month_at_runtime = $mon + 1;
    my $day = $mday;
    my $timestamp = sprintf( "%02d%02d%02d",$hour, $min, $sec);
    my $datestamp = sprintf("%02d%02d%04d", $month_at_runtime, $day, $year+1900 );
    my $psvFileName = "clientdata".".".$ENV{'CORP'}.".".$ENV{'RUNDATE'}.".".$ENV{'CYCLE'}.".".$ENV{'PATH_CLIENT'}.".".$datestamp.$timestamp.".psv";
    my $psvFile = "$ENV{'WIP_DIR'}/$psvFileName";
    open my $PSVFILE, ">> $psvFile" or die "Unable to open $psvFile $!, stopped";
 
    my $index = 0;
   

     foreach my $mail_piece_href (@{$metadata_href->{'MAIL_PIECES'}}) {
            $index++;
            my $orderIdIndex = 0;			
            my $infactAcctNum = $mail_piece_href->{'infact_account_number'};
            my $padded_infactAcctNum = sprintf "%-50s", $infactAcctNum;
            print $PSVFILE "act|$padded_infactAcctNum|$index| \n" or &fail("Unable to write to '$psvFile': $!");

         foreach my $orderid_href (@{$mail_piece_href->{'pdf_documents'}}) {
            $orderIdIndex++;
            my $eachOrderId = $orderid_href->{'order_id'};
			if (defined $eachOrderId and length $eachOrderId) {
				print $PSVFILE "nop|orderId$orderIdIndex|$eachOrderId| \n" or &fail("Unable to write to '$psvFile': $!");
			}
			else {
				&fail("Missing required orderId for '$infactAcctNum': $!");
			}

        }


    }
    close $PSVFILE or &fail("Unable to close '$psvFile': $!");
    &fm_sub::publishToJob("RS_SIDE_FILE", "$psvFile");
    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

sub exec_send_reporting_solutions_side_file()
{
    my $config_file_name = "file_transfer.cfg";
    if (! -e "$ENV{CONFIG_DIR}/$config_file_name")
    {
        fail ("ERROR: $config_file_name does not exist in $ENV{CONFIG_DIR}. Stopped\n");
    }
    my $psvfile = $ENV{'RS_SIDE_FILE'};
    my $donefile = "$psvfile.done";
    
    &exec_file_transfer_script('file_name' => "$psvfile", 'config_file_name' => "$ENV{CONFIG_DIR}/$config_file_name");
   
    open my $DONEFILE, ">> $donefile" or die "Unable to open $donefile $!, stopped";
    close $DONEFILE or &fail("Unable to close '$donefile': $!");
    
    &exec_file_transfer_script('file_name' => "$donefile", 'config_file_name' => "$ENV{CONFIG_DIR}/$config_file_name");
    
    &fm_sub::log_info("Job Step Completed Successfully\n");
    return $fm_sub::SUCCESS;
}

# =============================================================================================
# private subroutines
# =============================================================================================
sub standard_job_step_setup
{
    my ($check_for_input) = @_;
        # Basically, if the caller uses @INPUT_FILES, you'll want this to be true,
        # for job step resbumitability.
    my ($calling_package_name, $calling_sub_name) = (caller 1)[0,3];
    &fm_sub::log_info("=" x 80 . "\n");
    &fm_sub::log_info("${calling_package_name}::${calling_sub_name}:\n");

    # Each jobstep may run on a different FEP; so update the ticket generation 
    # environment to reflect the current FEP.
    chomp (my $host = `hostname`);
    &set_env_var('CLARIFY_PROCESSING_MACHINE', $host);
    # set input file names
    # (sort them so that the order of the statements in the output is 
    # consistent, given a consistent set of input files -- necessary for 
    # regression testing or simply diffing outputs)
    @INPUT_FILES = sort &fm_sub::get_file_list();
    &fail("Unable to obtain names of files registered to this FM event!") if ( !@INPUT_FILES );

    # log jobstep parameters
    &fm_sub::log_info("\n");
    &fm_sub::log_info("Program        : $0\n");
    &fm_sub::log_info("Process ID     : $$\n");
    &fm_sub::log_info("Started By     : $ENV{'LOGNAME'}\n");
    &fm_sub::log_info("Host           : " . $ENV{'CLARIFY_PROCESSING_MACHINE'});
    &fm_sub::log_info("\nJobstep was called with the following parameters: @ARGV\n");
    &fm_sub::log_info("   BASEPATH       : $ENV{'BASEPATH'}\n");
    &fm_sub::log_info("   RUNID          : $ENV{'RUNID'}\n");
    &fm_sub::log_info("   CORP           : $ENV{'CORP'}\n");
    &fm_sub::log_info("   RUN DATE       : $ENV{'RUNDATE'}\n");
    &fm_sub::log_info("   CYCLE          : $ENV{'CYCLE'}\n");
    &fm_sub::log_info("   WIP_DIR        : $ENV{'WIP_DIR'}\n");
    &fm_sub::log_info("   INPUT FILE(S)  : @INPUT_FILES\n\n");

    # Ensure we're in WIP dir by default before running any processes; so that core 
    # files, etc. are generated in the correct place.
    chdir "$ENV{'WIP_DIR'}" or &fail("Unable to change dir to $ENV{'WIP_DIR'}: $!");
    &fm_sub::log_info("The current working directory is: ".getcwd()."\n");

    # Replace failed file if needed or fail if not found at all.
    # Do this here only for files registered to FM for the current event; any 
    # other files which may have been moved to failed should be moved back into 
    # place by the job step which is about to use them; so that the knowledge of 
    # such files is only propogated to the subroutines that need them.
    &restage_input_files_if_needed(\@INPUT_FILES) if (defined $check_for_input and "$check_for_input");

    # Load common parameters into our environment:
    # All expected parameters for this event, where possible, should be set in the applicable 
    # custom_hook.cfg section.  The idea is to easily set and see, in the hook config, all the 
    # parameters affecting a given file/run and have them applied automatically here.
    # (example values might be: AFPGEN_MASTER_ELEMENT_CONFIG, DR_FORMATTER_CONFIG, INFACT_POOL, 
    # SEND_REPORTS [Y|N], RESOURCE_REVISON)
    # For more complicated systems with man differences per corp, a corp_map may be needed; in 
    # which case, the fm_custom_hook.cfg should have only parameters which are used when 
    # registering the file.  If those parameters need to be available to the subsequent jobs 
    # and are not available in the RUNID (in other words, not CORP, RUNDATE, CYCLE which are 
    # autopopulated into the environment for us), then this export will provide those values 
    # still, but all other corp-specific settings should be in the corp_map.cfg.  Please do not
    # additionally put the fm_custom_hook.cfg settings in question into the the corp_map.cfg, 
    # beause we do not want more than one source of any given setting (duplicating values is 
    # bad).
    my $file_name_to_match = $INPUT_FILES[0];
    &export_hook_cfg_info($file_name_to_match);

    &fm_sub::log_info("\n" . '=' x 55 . "\n\n");
    &fm_sub::log_info("\nAfter loading config files and setting defaults, the complete environment is:\n");
    &fm_sub::log_info(join "\n", map {"$_=$ENV{$_}"} sort keys %ENV);
    &fm_sub::log_info("\n" . '=' x 55 . "\n\n");

    # set any other file-scoped values
    if ($ENV{'event_type_id'} eq $ENV{'PDF_PREPROC_RUN_EVENT_ID'}) {
        $CORP_MAP_REF = &get_corp_map("$ENV{'CLIENT_ID'}","$ENV{'APP_IDENTIFIER'}");
        &check_for_basic_needed_env_vars();
        $SEND_PRINT_FILE             = (defined $ENV{'SEND_PRINT_FILE'}    and $ENV{'SEND_PRINT_FILE'}      =~ /NO/i) ? 0 : 1;
        $DO_BILLING                  = (defined $ENV{'DO_BILLING'}         and $ENV{'DO_BILLING'}           =~ /NO/i) ? 0 : 1;
        $SEND_CSR_REPORTS            = (defined $ENV{'SEND_CSR_REPORTS'}   and $ENV{'SEND_CSR_REPORTS'}     =~ /NO/i) ? 0 : 1;
        $SEND_TO_UR_LOADER           = (defined $ENV{'SEND_TO_UR_LOADER'}  and $ENV{'SEND_TO_UR_LOADER'}    =~ /NO/i) ? 0 : 1;
        $PRODUCT_SPECIFIC_CONFIG_DIR = "$ENV{'BASEPATH'}/system/config/" . $CORP_MAP_REF->{'config_subdir'};
    }

    # create any non-standard dirs used by multiple job steps here:
    #&ensure_dir_exists("$ENV{'BASEPATH'}/input/hold/blah"); # just an example; comment out if not needed

    # Do not send any output other than print when running in DR mode.
    #if (&running_in_DR_mode()) {
    #    &fm_sub::log_info("DR mode detected; will refrain from sending all output other than print.  Setting SEND_METHOD=null so that all output files are still prepared and archived as usual.  This allows us to manually send the output files when DR mode ends.");
    #    &set_env_var('SEND_METHOD', "null");
    #}

    &reobtain_job_lock_if_needed();
}

sub dump_environment_to
{
    my ($properties_file) = @_;
    my ($key, $val);

    open(PROPS, ">$properties_file") or &fail("Failed to open $properties_file $!");

    foreach $key ( sort( keys %ENV ) ) {
        $val = $ENV{$key};
        print PROPS "ENV.$key=$val\n" or &fail("Unable to write to $properties_file $!");
    }
    close(PROPS) or &fail("Failed to close $properties_file $! Error occurred");
}

sub prepare_master_config
    # To allow environment vars to be used in master config, we copy the config to WIP and 
    # replace any "${NAME}" strings with the value of $ENV{'NAME'}.
{
    my ($config_template, $wip_dir) = @_;

    my $config = "$wip_dir/" . &basename("$config_template");
    &fm_sub::log_info("Copying afpgen config, ${config_template}, to WIP...");
    &copy($config_template, $config) or &fail("Unable to copy $config_template to $config: $!");
    &set_env_var("AFPGEN_WIP_DIR", "$wip_dir");
    &fm_sub::log_info("Replacing any variables in the WIP copy of afpgen config...");
    &replace_vars_in_file($config);

    return $config;
}

sub slurp
{
    my ($file) = @_;

    open(my $fh, "<", "$file") or &fail("Unable to open $file $!\n");
    local $/; # set record separator to null
    my $content = <$fh>;
    &fail("Failed to read from $file: $!\n") if (not defined $content);
    close($fh); # don't care if this fails because we already read successfully

    return "$content";
}

sub get_billing_info
{
    my ($file) = @_;
    my %billing_info = ();

    open my $in, "< ${file}" or &fail("Unable to open ${file} for reading.\n");
    until (eof($in)) {
        defined($_ = <$in>) or &fail("Unable to read from ${file}: $!");

        if (/quantity:(\d+)/) {
            $billing_info{'QUANTITY'} = $1;
        }
        elsif (/corp_number:(\w+)/) {
            $billing_info{'CORP'} = $1;
        }
        elsif (/run_date:(\w+)/) {
            $billing_info{'RUNDATE'} = $1;
        }
        elsif (/ibs_cycle:(\w+)/) {
            $billing_info{'CYCLE'} = $1;
        }
        elsif (/billing_code:(\w+)/) {
            $billing_info{'BILLING_CODE'} = $1;
        }
    }
    close $in;

    return %billing_info;
}

sub get_billing_api_files
    # TODO: billing codes need to be in afpgen.cfg
{
    my (@billing_files) = glob("$ENV{'WIP_DIR'}/*.btrn_api.*"); # NOT *.btrn.* !!
}

sub get_next_cleanup_rundate
{
    my $subroutine_name = (caller 0)[3];
    my ($formatID, $eventID, $bufferMinutesAllowed) = @_;

    my $local_timezone = &get_system_timezone();
    my ($currentTime, $todaysRunTime, $chosenRunTime);

    # Construct epoch times for the current time and today's run time
    # for the timezone in which the run will take place.
    my ($runHour, $concat_timezone) = &get_event_schedule($formatID, $eventID);
    $ENV{'TZ'} = $concat_timezone; tzset;
    $currentTime = $SECONDS_SINCE_UNIX_EPOCH;
    my ($currentDay, $currentMonth, $currentYear) = (localtime($currentTime))[3,4,5]; # uses TZ value
    $todaysRunTime = &timelocal(0,0,$runHour,$currentDay,$currentMonth,$currentYear); # uses TZ value
    my $formattedSchedRunTime = localtime($todaysRunTime);
    my $formattedCurrentTime = localtime($currentTime);
    $ENV{'TZ'} = $local_timezone; tzset; # restore local timezone immediately so that the timestamps on any following log entries are correct
    &fm_sub::log_info("$subroutine_name: Determined today's scheduled run time to be: $formattedSchedRunTime, $concat_timezone\n");
    &fm_sub::log_info("$subroutine_name: Current time is: $formattedCurrentTime, $concat_timezone\n");

    # Select the next run, or, if within $bufferMinutesAllowed minutes of the next run,
    # choose the one after the next instead.
    if( ($todaysRunTime - $currentTime) < ($bufferMinutesAllowed * $NUM_SECONDS_PER_MINUTE) ) {
        $chosenRunTime = $todaysRunTime + $NUM_SECONDS_PER_DAY;
    }
    else {
        $chosenRunTime = $todaysRunTime;
    }
    &fm_sub::log_info("$subroutine_name: Assigning file to the following runtime: " . localtime($chosenRunTime) . ", $local_timezone\n");

    # save run info to environment
    $ENV{'EPOCH_RUNTIME'} = $chosenRunTime;
    $ENV{'TIMEZONE'} = $concat_timezone;

    # build and return rundate string
    $ENV{'TZ'} = $concat_timezone; tzset;
    my ($mday,$mon,$year) = (localtime($chosenRunTime))[3,4,5];
    $ENV{'TZ'} = $local_timezone; tzset; # restore local timezone immediately so that the timestamps on any following log entries are correct
    my $runDate = sprintf("%02d%02d%04d", $mon+1, $mday, $year + 1900);
    return($runDate);
}

sub get_event_schedule
    # returns (run_hour, timezone)
{
    my ($formatID, $eventID) = @_;
    my ($runHour, $timezone, $currentHour);

    my @result = &fm_sub::getFileGroupSubmitTime($formatID, $eventID);
    if(! @result) {
          die "Unable to get file group submit time from FM for event '$eventID', file format id '$formatID'.  Either no hour is configured for the file group in the FM setup, or the format ID/event type combination is invalid.";
    }
    elsif (@result == 1) {
        # "some kind of data error with the hour or time zone string on the group" -- FM API primer
        my $desc = $result[0];
        die "Unable to get file group submit time from FM for event '$eventID', file format id '$formatID': $desc";
    }
    else {
         ($runHour, $timezone, $currentHour) = @result;
         &fm_sub::log_info("FM says that in $timezone, the current hour is $currentHour and the hour of the group run is $runHour\n");
     }

    return ($runHour, $timezone);
}

sub get_reporting_solutions_snode
{
        my $snode = "";
        if( (defined($ENV{'STAGE'})) && ($ENV{'STAGE'} =~ "dev|qa|uat") )
        {
           $snode = "sterlingqa";
        }
        elsif ((defined($ENV{'STAGE'})) && ($ENV{'STAGE'} =~ "prod"))
        {
           $snode = "edhusiosp";
        }
        &fm_sub::log_info("NDM file with SNODE: $snode \n");
    
    return $snode;

}

sub get_reporting_solutions_snode_id
{
    my $snodeid = "";
        if ((defined($ENV{'STAGE'})) && ($ENV{'STAGE'} =~ "prod"))
        {
             $snodeid = "odsusr01";
        }
        if( (defined($ENV{'STAGE'})) && ($ENV{'STAGE'} =~ "dev|qa|uat") )
         {
             $snodeid = "cdusr721";
        }
          
        &fm_sub::log_info("NDM file with SNODEID: $snodeid \n");
       return $snodeid;
}


1;

# ======================================================================== 
# vim autosettings ("set modeline" in vimrc to enable) 
# vi: set expandtab sts=4 ts=4 sw=4 tw=95 fileencoding=utf-8 : 
# ======================================================================== 

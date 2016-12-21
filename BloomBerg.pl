#!perl -w
use strict;
use Encode;
use Locale::Country;
use WWW::Mechanize;
use Time::Piece::MySQL;
use HTML::TableExtract;
use HTML::Scrubber;
use DBI;
my $timeNow = localtime;
my $credt_date =  $timeNow->mysql_datetime;
my %configData1 = &Config_data();
my $dbi = "dbi:mysql:database="
        . ${configData1}{'DBCreds'}{'DBName'} . ";host=" 
        . ${configData1}{'DBCreds'}{'DBHost'};
my $dbh = DBI->connect($dbi ,
                       ${configData1}{'DBCreds'}{'DBUser'},
                       ${configData1}{'DBCreds'}{'DBPass'},
                       {RaiseError => 1}) || die "Cannot Connect: $DBI::errstr";
my @countries = &exculsion_array();
my $adver_info = &read_advertiser_info();
###############################################################################
my $topLink = "http://careers.bloomberg.com/jscommon/hire/hire_jobs.js";
my $browser = WWW::Mechanize->new();
$browser->get($topLink);
my $content = decode_utf8($browser->content());
open FW, ">java.txt" or die "cannot write on the disk\n$!\n";
printf FW $content;
close FW;
#$|=1;
open FR, "java.txt" or die "cannot read from disk\n$!\n";
my %e;
print "Accessing Bloomberg site\n";
my ($key, $next);
while(<FR>){
    if($_ =~ m!(\w+)\s=\s\[\s+\'(.*)\',!i){
        $key = $1;
        $next =  $2;
        $key = &clearit($key);
        $next = &clearit($next);
        push(@{$e{$key}}, $next);
    }elsif($_ =~ m!\s+\'(.*)\'!){
        my $nexts = $1;
        $nexts = &clearit($nexts);
        push @{$e{$key}}, $nexts;
    }
}
close FR;
sub clearit(){
    my $data = shift;
    $data =~ s!^\s+(.*)!$1!i;
    $data =~ s!(.*)\s+$!$1!i;
    $data =~ s!(&amp;|\n)!!i;
    return $data;
}
my $d = $e{'arRegions'};#getting array values of the arRegion

#arExperience arRegions arTitles arReqnum arSpeciality arJobfunction arLocations
#my($z1,$z2,$z3,$z4,$z5,$z6,$z7);
#$z1 = $e{'arExperience'};
#$z2 = $e{'arRegions'};
#$z3 = $e{'arTitles'};
#$z4 = $e{'arReqnum'};
#$z5 = $e{'arSpeciality'};
#$z6 = $e{'arJobfunction'};
#$z7 = $e{'arLocations'};
#foreach my $x($z1,$z2,$z3,$z4,$z5,$z6,$z7){
#    my $scal =  $x;
#    my $scal1 = @$scal;
#    print "-$scal1-";
#}


my $count = 0;
for my $kt(@$d){
    #print "\nCOUNTER: $count\n";
    if($kt eq 'Europe, Middle East and Africa'){
        my %hash = &get_index($count);
        &insertingSub(\%hash);
    }
    $count++;
}
#
sub get_index(){
        my $num = shift;
        #print "\nNumber: $num\n";
    my %dataHash;
        my $jobtitle = $e{'arTitles'}[$num];
        #$jobtitle = $1 if($jobtitle =~ m!^Bloomberg\s+(.*)!i);
        $dataHash{'vac_job_title'} = &clearit($jobtitle);
        $e{'arLocations'}[$num] =~ m/(.*) - (.*)/i;
        my $location = $1;
        my $country = $2;
        my $country_code = country_code2code(
            $country, LOCALE_CODE_ALPHA_3,LOCALE_CODE_ALPHA_2);
        $country_code = "UK" if ($country_code =~ m!(gb|GB)!);
        $dataHash{'vac_country'} = &clearit($country_code);
        $dataHash{'vac_locn'} = &clearit($location);
        my $refNo = $e{'arReqnum'}[$num];
        $dataHash{'vac_advjob'} = &clearit($refNo);
        my $jobUrl = "http://careers.bloomberg.com/hire/jobs/job" . $e{'arReqnum'}[$num] . ".html";
        
        $dataHash{'vac_url'} = &clearit($jobUrl);
        my $jobText = &jobdesc($jobUrl);
        my $jobDesc = $$jobText[1];
        my $pageContent = $$jobText[0];
        $dataHash{'vac_text'} = $pageContent;
        $dataHash{'vac_jd'} = $jobDesc;
        $dataHash{'arRegions'} = $e{'arRegions'}[$num];
        print "\nJob ref No. $refNo. Processed.\n***********\n";
       return %dataHash;
}

sub jobdesc(){
    my $jobPage = shift;
    my $text;
    $browser->get($jobPage);
    my $pageLink = decode_utf8($browser->content());
    #print $pageLink;
    push @$text, $pageLink;
    my $te = HTML::TableExtract->new(depth=>1, count=>1);
    $te->parse($pageLink);
    my $string;
    foreach my $ts ($te->tables) {
        foreach my $row ($ts->rows) {
            foreach(@$row){
                $string .= $_;
            }
        } 
     }
    sleep(5);
     my $scrb = HTML::Scrubber->new();
     $string = decode_utf8($string);
     $string = $scrb->scrub($string);
    
     if($string =~ m!(.*)!){
        $string =~ s!^\n\s+!!ig;
        $string =~ s!(\n+|\s+)$!!ig;
     }
     push @$text, $string;
     return $text;
}
###############################################################################
sub insertingSub(){
    my $each = shift;
    my %configData = &Config_data();
    my %eachJob = %$each;
            #while(my($name,$value) = each(%eachJob)){
            #    next if $name eq 'vac_text';
            #    print "\n$name:\n$value\n*************\n";
            #}
    #print "\nJobRef:$eachJob{'vac_advjob'}\n**\n,JobTitle:$eachJob{'vac_job_title'}\n**\n,Location:$eachJob{'vac_locn'}\n**\n,URL:$eachJob{'vac_url'}\n**\n,Country:$eachJob{'vac_country'}\n**\n";
    if (grep $_ eq $eachJob{'vac_country'},@countries){
        print "\nCountry in the Exclusion List Found, Moving to next Job\n^^^^^^^^^^^^^\n";
        return;
    }elsif (grep $_ eq $eachJob{'vac_locn'},@countries){
        print "\nCountry in the Exclusion List Found, Moving to next Job\n^^^^^^^^^^^^^\n";
        return;
    }else{  #checking if scrape vacancis has duplicate data;

        my $checkScrape = $dbh->prepare("SELECT * from scrape_vacancies where sv_ref=? and sv_script=?");
        $checkScrape->execute($eachJob{'vac_advjob'} , ${configData}{'Script'}{'NAME'});
        my ($rows) = $checkScrape->fetchrow_array;
        if ($rows){#
            print "\nData Already Exists, Moving to next Record\n====================\n";
            return;
        }else{
            print "\nProcessing Job Ref NO. $eachJob{'vac_advjob'} .\n";
            my $insertJobSql =$dbh->prepare(
                        "INSERT INTO vacancy
                        (
                            vac_cre_dte,
                            vac_status,
                            vac_job_title,
                            vac_advertiser,
                            vac_phone,
                            vac_contact,
                            vac_title,
                            vac_needed,
                            vac_duration,
                            vac_dur_type,
                            vac_add1,
                            vac_add2,
                            vac_add3,
                            vac_add4,
                            vac_pcode,
                            vac_machine,
                            vac_modified,
                            vac_user,
                            vac_text,
                            vac_locn,
                            vac_type,
                            vac_salary,
                            vac_advjob,
                            vac_effective,
                            vac_email,
                            vac_url,
                            vac_jd,
                            vac_lsource,
                            vac_f_source,
                            vac_f_sector,
                            vac_f_sector2,
                            vac_f_sector3,
                            vac_country)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                            SCRAPEDATA:
                            eval{
                                $insertJobSql->execute(
                                $credt_date,
                                1,
                                $eachJob{'vac_job_title'},
                                ${configData}{'Advertiser'}{'ADREF'},
                                $adver_info->[1],
                                $adver_info->[1],
                                $adver_info->[2] || "N",#if none found
                                0,
                                0,
                                1,
                                $adver_info->[3],
                                $adver_info->[4],
                                $adver_info->[5],
                                $adver_info->[6],
                                $adver_info->[7],
                                "Web",
                                $credt_date,
                                "Robot",
                                $eachJob{'vac_text'},
                                $eachJob{'vac_locn'},
                                "P",
                                "",#<-$eachJob{'vac_salary'},
                                $eachJob{'vac_advjob'},
                                $credt_date,
                                $adver_info->[8],
                                $eachJob{'vac_url'},
                                $eachJob{'vac_jd'},
                                "http://careers.bloomberg.com/hire/experiencesearch.html",
                                1,
                                ${configData}{'Advertiser'}{'SEC_1'},
                                ${configData}{'Advertiser'}{'SEC_2'},
                                ${configData}{'Advertiser'}{'SEC_3'},
                                $eachJob{'vac_country'},
                                ) };
            if($@){
                print "\nThe Following Error \n$@\n Retrying Data insertion in table 'vacancy' !\n";
                goto SCRAPEDATA;
            }else{                        
                print "\nJob ref No. $eachJob{'vac_advjob'} added into vacancy table.\n+++++++++++++\n ";
            }
            my $lastId = $dbh->prepare("SELECT vac_ref from vacancy where vac_advjob=? and vac_cre_dte=?");
            $lastId->execute($eachJob{'vac_advjob'},$credt_date) or die "Cannot Execute get_new_vacancy_id\n$!";
            my $last_id = ($lastId->fetchrow_array())[0];
            my $scrape = $dbh->prepare("INSERT INTO scrape_vacancies (sv_script, sv_ref, sv_date, sv_our_ref) VALUES (?, ?, ?, ?) ");
            SCRAPE_VACANCY:
            eval{$scrape->execute($configData{'Script'}{'NAME'}, $eachJob{'vac_advjob'},$credt_date, $last_id)};
                if($@){
                    #print "\n$configData{'Script'}{'NAME'}, $eachJob{'vac_advjob'},$credt_date, $new_vacRef\n";
                    print "\nError \n$@\n retrying inserting data into scrape_vacancies\n";
                    goto SCRAPE_VACANCY;
                }else{
                    print "\nData added to scrape_vacancies table.\n------------------\n";
                }
        }
        
    }
1;
}

sub Config_data(){
    open CD, "Config.ini" or die "Cant open Config.ini file\n";
    my %heading;
    my $hashname;
    while(<CD>){
        if($_ =~ m/\[(.*)\]/){
            $hashname = $1;
        $heading{$hashname} = {};
        }elsif($_ =~ m/^\w/){ 
            my($key, $value) = split/=/;
            $key = &scrubIt($key);
            $value = &scrubIt($value);
            $heading{$hashname}{$key} = $value;
        }
    }
    close CD;
    return %heading; 
}

sub scrubIt(){
    my $string = shift;
    chomp($string);
    $string =~ s/^(\s+|\t+)//ig;
    $string =~ s/(\s+|\t+)$//ig;
    return $string;
}
sub exculsion_array(){
    my $file = "Exclusion.txt";
    my @country_list;
    open FH, $file or die "$!";
    while(<FH>){
        $_ =~ s!(\s|\n|\t)!!ig;
        push @country_list, $_;
    }
    return @country_list;
    close FH;
}

sub read_advertiser_info(){
    my $info = $dbh->prepare("SELECT
                                    adv_tel,
                                    adv_cont_1,
                                    adv_title,
                                    adv_add1,
                                    adv_add2,
                                    adv_add3,
                                    adv_add4,
                                    adv_pcode,
                                    adv_email,
                                    adv_fax_1
                             from advertiser
                             WHERE adv_ref = ?
                             ORDER BY adv_ref
                             DESC LIMIT 1");
     $info->execute(${configData1}{'Advertiser'}{'ADREF'}) or die "Cannot Execute read_advertiser_info\n$!";
 my $result = $info->fetchrow_arrayref;
    return $result;
}

  
  

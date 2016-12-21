#!perl -w
use strict;
use Time::Piece::MySQL;
use List::MoreUtils qw(uniq);
use DBI;
use tpcareer_helper;
my $timeNow = localtime;
my $credt_date =  $timeNow->mysql_datetime;
my %configData = &Config_data();
my @countries = &exculsion_array();
my $path = "http://ig29.i-grasp.com/fe/tpl_travisperkins01.asp?newms=se";
my $dbi = "dbi:mysql:database="
        . ${configData}{'DBCreds'}{'DBName'} . ";host=" 
        . ${configData}{'DBCreds'}{'DBHost'};
my $dbh = DBI->connect($dbi ,
                       ${configData}{'DBCreds'}{'DBUser'},
                       ${configData}{'DBCreds'}{'DBPass'},
                       {RaiseError => 1}) or die "Cannot Connect: $DBI::errstr";
my $adver_info = &read_advertiser_info();
#print "\n$adver_info->[0],$adver_info->[1],$adver_info->[2],$adver_info->[3],$adver_info->[4],$adver_info->[5],$adver_info->[6],$adver_info->[7],$adver_info->[8],$adver_info->[9],$adver_info->[10],\n";
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
     $info->execute(${configData}{'Advertiser'}{'ADREF'}) or die "Cannot Execute read_advertiser_info\n$!";
 my $result = $info->fetchrow_arrayref;
    return $result;
}


#program starts#
my @l = &get_job_links($path);
@l = uniq(@l);#find & remove duplicate link of a job 
my $c = 1;
my $total_jobs_links = @l;
print "\nWe have extracted links for $total_jobs_links jobs.\n";
for my $ll(@l){  
my %eachJob = &each_job_page($ll);
if (%eachJob){
 
    if (grep $_ eq $eachJob{vac_country},@countries){
        print "\nCountry in the Exclusion List Found, Moving to next Job\n";
        next;  
        }else{  #checking if scrape vacancis has duplicate data;
            my $checkScrape = $dbh->prepare("SELECT * from scrape_vacancies where sv_ref=? and sv_script=?");
            my $result = $checkScrape->execute($eachJob{'vac_advjob'} , ${configData}{'Script'}{'NAME'});
            my ($rows) = $checkScrape->fetchrow_array;
                if ($rows) {
                    print "\nData Already Exists, Moving to next Record\n====================\n";
                    next;
                }else{
                    my $insertJobSql =
                                $dbh->prepare(
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
                                VALUES          (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
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
                               $eachJob{'vac_machine'},
                                $credt_date,
                                $eachJob{'vac_user'},
                                $eachJob{'vac_text'},
                                $eachJob{'vac_locn'},
                                $eachJob{'vac_type'},
                                $eachJob{'vac_salary'},
                                $eachJob{'vac_advjob'},
                                $credt_date,
                                $adver_info->[8],
                                $eachJob{'vac_url'},
                                $eachJob{'vac_jd'},
                                $eachJob{'vac_lsource'},
                                1,
                                ${configData}{'Advertiser'}{'SEC_1'},
                                ${configData}{'Advertiser'}{'SEC_2'},
                                ${configData}{'Advertiser'}{'SEC_3'},
                                $eachJob{'vac_country'}
                                ) };
                                if($@){
                                    print "\nThe Following Error \n$@\n Retrying Data insertion in table 'vacancy' !\n";
                                    goto SCRAPEDATA;
                                }else{
                                    
                                    print "\nJob Added into vacancy table.\n ";
                                }

    my $lastId = $dbh->prepare("SELECT vac_ref from vacancy where vac_advjob=? and vac_url=?");
    $lastId->execute($eachJob{'vac_advjob'},$eachJob{'vac_url'}) or die "Cannot Execute get_new_vacancy_id\n$!";
    my $last_id = ($lastId->fetchrow_array())[0];
                    my $scrape = $dbh->prepare("INSERT INTO scrape_vacancies (sv_script, sv_ref, sv_date, sv_our_ref) VALUES (?, ?, ?, ?) ");

                    SCRAPE_VACANCY:
                    eval{$scrape->execute($configData{'Script'}{'NAME'}, $eachJob{'vac_advjob'},$credt_date, $last_id)};
                    if($@){
                        #print "\n$configData{'Script'}{'NAME'}, $eachJob{'vac_advjob'},$credt_date, $new_vacRef\n";
                        print "\nError \n$@\n retrying inserting data into scrape_vacancies\n";
                        goto SCRAPE_VACANCY;
                    }else{
                        print "\nData added to scrape_vacancies table.\n";
                    }
                }
        }
    }else{
        print "Hash is empty";
    }
}
    

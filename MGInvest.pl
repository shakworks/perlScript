#!perl -w
use strict;
use Encode;
use Locale::Country;
use WWW::Mechanize;
use WWW::Mechanize::DecodedContent;
use Time::Piece::MySQL;
use HTML::TableExtract;
use HTML::TreeBuilder;
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
my $masterLink = "https://www.amris.com/wizards/mandgmain/";
my $browser = WWW::Mechanize->new();
#$browser->agent_alias('Linux Mozilla');
#$browser->add_header('Accept-Charset' => 'iso-8859-1');
my $link = $masterLink . "vacancySearch.php";
print "$link \n Searching the Site for all the Jobs.\n";
my $page = $browser->get($link);
my $content = $page->decoded_content();
my $links = HTML::TreeBuilder->new();
my @links;
$links->parse($content);
    foreach my $b( $links->find_by_tag_name('a')){#codeListValue
        if($b->as_text eq "...more"){
          push @links, $b->attr('href');
        }
    }
my $totalJobs = @links;
$links->eof;
$links->delete;
print "there are total $totalJobs jobs.";
for my $eac (@links){
#(0SourceCode, 1JobTitle, 2Type, 3Location,
#4ContractType, 5RefId, 6CloseDate, 7JobDesc, 8jobLink, 9salary)
    my @jobdata = &JobDescription($eac);
    my %jobData;
    $jobData{'vac_job_title'} = $jobdata[1];
    $jobData{'vac_locn'} = $jobdata[3];
    $jobData{'vac_text'} = $jobdata[0];
    if($jobdata[4] eq "Permanent"){
        $jobData{'vac_type'} = "P";
    }elsif($jobdata[4] eq "Temporary"){
        $jobData{'vac_type'} = "T";
    }
    $jobData{'vac_country'} = $jobdata[3];
    $jobData{'vac_jd'} = $jobdata[7];
    $jobData{'vac_advjob'} = $jobdata[5];
    $jobData{'vac_country'} = $jobdata[3];
    $jobData{'vac_url'} = $jobdata[8];
    $jobData{'vac_salary'} = $jobdata[9];
    #while(my($name , $value) = each(%jobData)){
    #  next if($name eq 'vac_text');
    #  next if($name eq 'vac_jd');
    #    print "\n======>$name\n$value\n=========\n";
    #}
    print "-- $jobdata[9] --\n";
    &insertingSub(\%jobData);
    sleep(5);
}

sub JobDescription(){
    my @returnData;
    my $jobLink = shift;
    my $masterLink = "https://www.amris.com/wizards/mandgmain/";
    my $job_link = $masterLink.$jobLink;
    print "\n$job_link\n Accessing to scrape job details.\n";
    sleep(2);
    my $jobpage = $browser->get($job_link);
    my $pagecontent = $jobpage->decoded_content();
    
    push @returnData, $pagecontent;
    my $links1 = HTML::TreeBuilder->new();
    $links1->parse($pagecontent);
    $links1->eof;
    my @b = $links1->find_by_tag_name('div');
    my $aa = @b ;
    foreach my $b1(@b){
        if($b1->attr('style')){
          if($b1->attr('style') eq "width:200px;font-size: 1.1em;"){
               push @returnData, $b1->as_text;
          }
        }
    }
    my @formDivs = $links1->find_by_attribute("class","formDiv");
    my $jobDesc = $formDivs[1]->as_text;
    #print $jobDesc;
    push @returnData, $jobDesc;
    my $salary;
    if($jobDesc =~ m!Salary: Market Rate$!i){
          $salary = "Market Rate";
    }else{
          $salary = " ";
    }
    $links->delete;
    push @returnData, $job_link;
    push @returnData, $salary;
    print "returning Data\n";
    return @returnData;
    #(SourceCode, JobTitle, Type, Location,
    #ContractType, RefId, CloseDate, JobDesc, JobLink, salary)
}

###############################################################
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
                                $eachJob{'vac_type'},
                                $eachJob{'vac_salary'},#<-$eachJob{'vac_salary'},
                                $eachJob{'vac_advjob'},
                                $credt_date,
                                $adver_info->[8],
                                $eachJob{'vac_url'},
                                $eachJob{'vac_jd'},
                                "https://www.amris.com/wizards/mandgmain/",
                                1,
                                ${configData}{'Advertiser'}{'SEC_1'},
                                ${configData}{'Advertiser'}{'SEC_2'},
                                ${configData}{'Advertiser'}{'SEC_3'},
                                ${configData}{'Advertiser'}{'COUNTRYCODE'},
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

  
  

#!perl -w
use strict;
use Encode;
use HTML::TableExtract;
use WWW::Mechanize;
use WWW::Mechanize::DecodedContent;
use HTML::TreeBuilder 3;
use HTML::Scrubber;
use Time::Piece::MySQL;
use DBI;
use List::MoreUtils qw(uniq);
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
my $masterLink = "https://www.asp14.bondtalent.com/hca/website";
my $searchLink = "/currentvacancies.asp";
$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;
my $browser = WWW::Mechanize->new();
$browser->agent_alias('Linux Mozilla');
$browser->add_header('Accept-Charset' => 'iso-8859-1');
my $link = $masterLink . $searchLink;
my $page = $browser->get($link);

my $content = decode_utf8($page->content());
#print $content;
my $searchpage = HTML::TreeBuilder->new();
$searchpage->parse($content);
$searchpage->eof;
foreach my $h1( $searchpage->find_by_tag_name('h1')){#codeListValue
        if($h1->as_text =~ m!^(\d+)\s!){
          my $jobs = $1;
          print "Total Jobs are $jobs\n";
        }
    }
my @pagesLink ;
my @bo= $searchpage->look_down("class","left ml25");
foreach my $node (@bo) {
  my @kids = $node->content_list( );
  my $b = 0;#counter
  for my $a(@kids){
       if (@kids and ref $kids[$b] and $kids[$b]->tag( ) eq 'a') {
          push @pagesLink, $kids[$b]->attr("href" );
     }
     $b++;
  }

}

@pagesLink = uniq(@pagesLink);#find and remove duplicate links
my $t = @pagesLink;
print "\nTotal $t pages to scrape job from.\n";
$searchpage->delete;
my @jobTotal;
for my $l(@pagesLink){#getting all job pages links from all the pages
    push @jobTotal, &LinkExtract($l);
}
@jobTotal = uniq(@jobTotal);
my $q = @jobTotal;
print "\nTotal jobs links equal to $q\n";
for my $eacJ(@jobTotal){
#EachJob(0sourcecode, 1Jobtitle, 2ref, 3area, 4category, 5ref, 6location, 7salary, 8closingdate, 9jobdetail, 10url, )
     print "Getting Job information for link:\n$eacJ\n";

      #&EachJob($eacJ);
     #while(my($n, $v) = each(%each_job)){
	#print "$n ----- $v\n";
     #}
     &insertingSub( &EachJob($eacJ) );
}


#########
#joblinks on each page#
#returns links of jobpages
sub LinkExtract(){
     my @link_list;
     my $link = shift;
     my $fxlink = $masterLink . "/" . $link;
     my $job_page = eval{$browser->get($fxlink)};
          if($@){
	print "Erorr:$@ \nCannot Access flink\n $fxlink";
	return;
     }
     my $list_content = decode_utf8($job_page->content());
     my $listtable = HTML::TableExtract->new(keep_html=>1);     
     $listtable->parse($list_content);
     #$listtable->parse_file($listPage);
     my $ftable = $listtable->first_table_found;
     my @hrefCol = $ftable->columns();
     my $sechref = $hrefCol[1];
     for my $col_item(@$sechref){
          if($col_item =~ m!^<p><a href='(.*)' title=!){
               my $link = $1;
               push @link_list, $link;
          }
     }
     return @link_list;
}



##########
#each Job#
#returns->(sourcecode, Jobtitle, ref, area, category, ref, location, salary, closingdate, jobdetail, url )
sub EachJob(){
     my $job_detail;
     my $link = shift;
     my $flink = $masterLink . "/" . $link;
     #my $file = "currentvacanciesjob1.asp";
     my $job_page = eval{$browser->get($flink)};
     if($@){
	print "Erorr:$@ \nCannot Access flink\n $flink";
	return;
     }
     my $job_content = decode_utf8($job_page->content());
     $job_detail->{'vac_text'} = $job_content;
     my $jobpage = HTML::TreeBuilder->new();
     $jobpage->parse($job_content);
     #$jobpage->parse_file($file);
     $jobpage->eof;
     foreach my $h1( $jobpage->find_by_tag_name('h1')){#codeListValue
	if($h1->as_text =~ m!(.*)\s+Details :$!){
	  my $title = $1;
	  $job_detail->{'vac_job_title'} = $title;
	}
     }
     my @allDesc= $jobpage->look_down("id","main_content");
     my $jobDesc ;
     foreach my $node (@allDesc) {
          my @kids = $node->content_list( );
          my $b = 0;#counter
               for my $a(@kids){
                    if (@kids and ref $kids[$b] and $kids[$b]->tag( ) eq 'h2') {
                         $jobDesc.= $kids[$b]->as_text . "\n";
                    }elsif(@kids and ref $kids[$b] and $kids[$b]->tag( ) eq 'p'){
                         $jobDesc.= $kids[$b]->as_text ."\n";
                    }
               $b++;
               }
     }
     $jobpage->delete;
#getting table info
     my $table = HTML::TableExtract->new();
     $table->parse($job_content);
     my $tableFound = $table->first_table_found;
     my @columns = $tableFound->columns();
     #0ref, 1area, category, 1ref, 3location, 4salary, 5closingdate
     my $fCol = $columns[0];
     my $sCol = $columns[1];
     my $counter = 0;
     foreach(@$fCol){
        if($fCol->[$counter] eq "Reference:"){
		$job_detail->{'vac_advjob'} = $sCol->[$counter] unless exists $job_detail->{'var'};
          }elsif($fCol->[$counter] eq "Area:"){
               $job_detail->{'vac_locn'} = $sCol->[$counter];
          }elsif($fCol->[$counter] eq "Salary:"){
               $job_detail->{'vac_salary'} = $sCol->[$counter];
          }
	$counter++;
     }
     $job_detail->{'vac_jd'} = $jobDesc;
     $job_detail->{'vac_url'} =  $flink;
     $job_detail->{'vac_country'} =  "UK";
     return $job_detail;

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
	    sleep(5);
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
                                "P",#$eachJob{'vac_type'},
                                $eachJob{'vac_salary'},#<-$eachJob{'vac_salary'},
                                $eachJob{'vac_advjob'},
                                $credt_date,
                                $adver_info->[8],
                                $eachJob{'vac_url'},
                                $eachJob{'vac_jd'},
                                "https://www.asp14.bondtalent.com/hca/website",
                                1,
                                ${configData}{'Advertiser'}{'SEC_1'},
                                ${configData}{'Advertiser'}{'SEC_2'},
                                ${configData}{'Advertiser'}{'SEC_3'},
                                ${configData}{'Advertiser'}{'COUNTRYCODE'},
                                ) };
            if($@){
                print "\nThe Following Error \n$@\n Retrying Data insertion in table 'vacancy' !\n";
		exit;
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

  
  

#!/usrbin/perl -w
use strict;
use Encode;
use WWW::Mechanize;
use HTML::TreeBuilder;
use HTML::Scrubber;
my $browser = WWW::Mechanize->new();
my $scrub = HTML::Scrubber->new();


            ######
#Getting job_links #
my $a = 0;
sub get_job_links(){
    my $link = shift;
    my $content;
    my $root = HTML::TreeBuilder->new;
    if($link eq "http://ig29.i-grasp.com/fe/tpl_travisperkins01.asp?newms=se"){
        $browser->get($link);
        $browser->submit_form(
            form_number => 1,
            fields      =>{ searchtext => 'a' }
            );
        $content = decode_utf8($browser->content()); 
    }else{
        $link = "http://ig29.i-grasp.com/fe/" . $link;
        my $page = eval{$browser->get($link)};
        warn $@ if $@;
        $content = decode_utf8($page->content());
    }
    $root->parse($content);
    $root->eof();
    
    my $nextPage;
    my @forward_links = $root->find_by_attribute("class", "ForwardBulletGif");
    foreach my $forward_link(@forward_links){
        my @forward = $forward_link->content_list();
        if(@forward and ref $forward[0] and $forward[0]->tag() eq 'a'){
            if($forward[0]->as_text eq 'Next Page'){
                $nextPage = $forward[0]->attr('href');
                unless($nextPage =~ m/http:\/\//){
                    #$nextPage = "http://ig29.i-grasp.com/fe/".$nextPage;
                    print "\n********\nNext Page link found.\n" . $nextPage;
                }
            }
        }
    }
    
  
  my $count = 0;  
  my @search_pages;
    my @asearch = $root->find_by_attribute("headers", "igSortBarTitle");
    foreach my $link(@asearch){
        my @links = $link->content_list();
        if(@links and ref $links[0] and $links[0]->tag() eq 'a'){
            #my $jobLink = "http://ig29.i-grasp.com/fe/" . $links[0]->attr('href');
            my $jobLink = $links[0]->attr('href');
            push @search_pages, $jobLink ;
            $count++;
        }
    }

    #my $jobs;
    #unless($a != 0){
    #    $a =1;
    #    my @jobs = $root->find_by_attribute("class", "igResultInfo");
    #    foreach my $node(@jobs){
    #        my @kids = $node->content_list();
    #        if(@kids and ref $kids[0] and $kids[0]->tag() eq 'strong'){
    #            my $text = $kids[0]->as_text();
    #            if($text =~ m/^Show(.*)\s(\d+)$/){
    #                $jobs = $2;
    #            }
    #        }
    #    }
    #    print "\nTotal number for jobs are mentioned on the first page are $jobs.\n";
    #}
   
    if(defined $nextPage){
        print "\nThere were $count job links found on this page\n**********\n";
        sleep(1);
        print "\nFor more Jobs links Accessing the next page.\n" . $nextPage;
        push @search_pages, &get_job_links($nextPage);
    }
    return @search_pages;

    
    
    $root->delete;
}
        ############
#Get Each Job details in Hash#
sub each_job_page(){
    my $jobslink = shift;
    my $job_link = "http://ig29.i-grasp.com/fe/" . $jobslink;
    my $root = HTML::TreeBuilder->new(api_version => 3);
    my %jobhash;
    my $p = eval{$browser->get($job_link)};
    warn $@ . "\nCould not Get \n $job_link\n***********************\n" if $@;

    if ($browser->status() ne 200){
        return %jobhash ;
    }
    print "Accessing Job page \n $job_link \n to get job information\n";
    my $content =  decode_utf8($p->content());
    
    $root->parse($content) or die "$!";
    $root->eof();
    my @vals;
    foreach my $b( $root->find_by_tag_name('dd')){#codeListValue
        push @vals, $b->as_text ;
    }
    foreach my $h($root->find_by_tag_name('h2')){
	my $title = $h->as_text;
	if($title =~ m!(.*)?\s-(.*)!i){
	   $title = $1; 
	}
	print "\nJob Title: $title\n";
        push @vals, $title;
    }
    $jobhash{'vac_status'} = 1;
    $jobhash{'vac_machine'} = "web";
    $jobhash{'vac_user'} = "Robot";
    $jobhash{'vac_advjob'} = $vals[0];
    $jobhash{'vac_locn'} = "$vals[2] , $vals[1]";#Town and County
    $jobhash{'Brand'} = $vals[3];
    $jobhash{'Bussiness Area'} = $vals[4];
    $jobhash{'vac_salary'} = $vals[5] || "Negotiable";
    $jobhash{'vac_job_title'} = $vals[6];
    $jobhash{'vac_country'} = "UK";
    $jobhash{'vac_f_source'} = 1;
    $jobhash{'vac_needed'} = 0;
    $jobhash{'vac_duration'} = 0;
    $jobhash{'vac_dur_type'} = 1;
    $jobhash{'vac_url'} = $job_link;
    $jobhash{'vac_lsource'} = "http://www.tpcareers.co.uk/";
    my $result = "";
    my $jobCondition = "P";
    if($content =~ m!(.*)<h3 id="igJobDesc0">(.*)</p><ul class="list_nobullet">(.*)!s) {
	$result = $2;
        #$result = $scrub->scrub($result);
        $result =~ s!(<span style="font-family: Arial">|<div>|</span>|</div>|<p>|&nbsp;)!!sg;
        $result =~ s!</p>!<br/>!sg;
        $result =~ s/<h3 id="(igJobDesc1|igJobDesc2)">/<h4>/sg;
        $result =~ s/<\/h3>/<\/h4>/sg;
        if(($jobhash{'vac_job_title'} =~ m/(Parttime|Part-Time)/i) or (($result =~ m/Part-Time/i))){
            $jobCondition = "C";
        }
        my $result = "<h4>" . $result;        
    } 
    $jobhash{'vac_jd'} = $result;
    $jobhash{'vac_type'} = $jobCondition;
    $jobhash{'vac_text'} = $content;
    print "Job information collected.\n";
    return %jobhash;
    $root->delete;
}


sub Config_data(){
    open CF, "Config.ini" or die "Cant open Config.ini file\n";
    my %heading;
    my $hashname;
    while(<CF>){
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
    return %heading;
    close CF;
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
1;
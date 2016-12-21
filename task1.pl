#!/usr/bin/perl -w
use strict;
use Win32::ODBC;
use CGI qw/:standard/;

#make,model,engine,version,doors) ukdisc and car_specs
my $dsn = "task";
my $db = new Win32::ODBC($dsn) or die "cant connect to database:::$!";

print header;

    print<<"EoF";
    <script>
function cookit(d){
document.cookie= 'name='+d ;
}
</script>
EoF


my $make = param('make');
my $model =param('model');

my $sql = "SELECT u.make,c.model,c.car_id,u.engine,u.version,c.doors,u.adjustment,c.uk_list,u.disc FROM ukdisc u INNER JOIN car_specs c ON  c.make=u.make  AND c.model=u.model AND c.engine=u.engine AND c.version=u.version AND c.doors=u.doors Where c.make='$make' AND u.model='$model' ORDER BY c.car_id";
$db->Sql($sql);
open (FH,"C:/Documents and Settings/shakir/Desktop/Task/prices.htm") or die "$!";
while(<FH>){
        if(/<make>/){
            $db->FetchRow();
            my %hash = $db->DataHash();
            print "$hash{make}";
        }
        elsif(/<results>/){
                 while($db->FetchRow){
                        my %hash = $db->DataHash();
                        my $reduced;
                        #my $discounted =int( $hash{uk_list} - (($hash{uk_list} * $hash{disc})/100));
                        #my $adj = int(($discounted * 2)/100);
                        #my $reduced;
                        my $percen= ($hash{uk_list}*$hash{disc}/100);
                        my $percent = $hash{uk_list}-$percen;
                        my $discounted= $percent + $hash{adjustment};
                        my $adj = int(($discounted * 2)/100);
                        if($adj > 470){
                            $reduced = int($discounted + $adj);
                         }else{
                            $reduced = $discounted + 470;
                         }
                    print "<tr><td><a href='/cgi-bin/task2.pl' onClick=cookit($hash{car_id});>$hash{make}&nbsp;$hash{model}$hash{version}$hash{engine}&nbsp;$hash{doors}&nbsp;doors...</td></a><td>$hash{uk_list}</td><td align='center'>$reduced</td></tr>\n";
                   }         
        }else{
        print $_;
        }
}
$db->Close();

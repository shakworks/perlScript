#!/usr/bin/perl -w
use strict;
use CGI qw/:standard/;
use Win32::ODBC;
print header;

my $l = cookie('name');
my $dsn = "task";
my $db = new Win32::ODBC($dsn) or die "Cant connect$!";

my $sql = "SELECT car_specs.car_id, car_specs.make, car_specs.model, car_specs.engine, car_specs.version, car_specs.doors, car_specs.uk_list, ukdisc.disc, ukdisc.adjustment, uk_options.option_name, uk_options.option_price
FROM ukdisc INNER JOIN (car_specs INNER JOIN uk_options ON car_specs.car_id = uk_options.car_id) ON (ukdisc.doors = car_specs.doors) AND (ukdisc.version = car_specs.version) AND (ukdisc.engine = car_specs.engine) AND (ukdisc.model = car_specs.model) AND (ukdisc.make = car_specs.make)
WHERE (((car_specs.car_id)=$l));
";
$db->Sql($sql);

open FH,"C:/Documents and Settings/shakir/Desktop/TAsk2/options.htm" or die "cant open the file $!";
while(<FH>){
    if(/<vehicle>/){
            $db->FetchRow();
                my %hash = $db->DataHash();
                print "$hash{make} $hash{model} $hash{engine} $hash{version} $hash{doors} doors...\n";
    }
    elsif(/<price>/){
        
        $db->FetchRow();
         my %hash = $db->DataHash();
                         my $discounted =int( $hash{uk_list} - (($hash{uk_list} * $hash{disc})/100));
                         my $adj = int(($discounted * 2)/100);
                         my $reduced;
                         
                         if($adj > 470){
                            $reduced = $discounted + $adj;
                         }else{
                            $reduced = $discounted + 470;
                         }
                         print "$reduced\n";
    }
    elsif(/<options>/){print "<table>";
        while($db->FetchRow){
            my %hash = $db->DataHash();
            my $discount = $hash{option_price} * $hash{disc}/100;
            my $price= $hash{option_price} - $discount;
            print "<tr><td>$hash{option_name}</td><td>$price</td></tr>\n";
        }print"</table>";
    }
    else
    {
        print $_;
    }
}        
$db->Close();

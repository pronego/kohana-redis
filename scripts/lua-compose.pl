#!/usr/bin/perl

use Cwd;
use Cwd 'abs_path';
use File::Basename;

my $file = $ARGV[0];

if ($file =~ /^[^\/]/) {
	$file = cwd()."/".$file;
}

if ( ! -e $file) {
	die("No such file: ".$file);
}

my $already_included_files = ();

sub compose_script {
    my $file = abs_path($_[0]);

    if ($_[1] and $file ~~ @already_included_files) {
        return "";
    }

    push(@already_included_files, $file);

    my $basename = basename($file);
    my $dirname  = dirname($file)."/";

    my $content;

    open(my $fh, '<', $file) or die "Cannot open file $file"; {
        local $/;
        $content = <$fh>;
    }
    close($fh);

    if ($content =~ /^(require(_once)?)\s+\"(.+?)\"$/m) {
        $content =~ s/^(require(_once)?)\s+\"(.+?)\"$/{compose_script($dirname.$3.'.lua', $2)}/meg;

        return $content;
    }
    else {
        return $content;
    }
}

print compose_script($file);

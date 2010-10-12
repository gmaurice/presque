#!/usr/bin/perl

use warnings;
use strict;
use Benchmark qw/:all/;
use lib 'lib';
use Test::More;
use Plack::Test;
use presque;

use JSON;
use HTTP::Request;

$Plack::Test::Impl = 'Server';

my $app = presque->app(
    config => {
        redis => {
            host => '127.0.0.1',
            port => 6379
        }
    }
);

my $queue            = "presque_test";
my $worker_id        = "worker_foo";
my $queue_url        = "http://localhost/q/$queue";
my $queue_batch_url  = "http://localhost/qb/$queue";
my $job_url          = "http://localhost/j/$queue";
my $status_url       = "http://localhost/status/$queue";
my $worker_stats_url = "http://localhost/w/?queue_name=$queue";
my $worker_url       = "http://localhost/w/";
my $control_url      = "http://localhost/control/$queue";

test_psgi $app, sub {
    my $cb = shift;
    my ($req, $res);
    my $content;
    my $i = 0;
    
    my $results = timethese( 1000, {
    	'00_create_job'	=> sub { create_job($cb, { job => "ID$i"}) ; $i++; },
    	'01_get_job' => sub { get_job($cb) ; $i = 0},
        '02_create_job_with_depends'	=> sub { 
            create_job($cb, { job => "ID$i"}, $queue_url."?depends=UNIQ_". ($i + 1)."&uniq=UNIQ_$i" ) if $i < 999;
            create_job($cb, { job => "ID$i"}, $queue_url."?uniq=UNIQ_$i" ) if $i == 999;
            $i++; },
        '03_get_job_with_depends' => sub { get_job($cb)->content ; $i = 0},
    });
    
    # flushing depending jobs not previously flushed
    do{
        $content = get_job($cb)->content;
        #warn $content;
    }while( JSON::decode_json($content)->{job} );
    
    cmpthese($results);

    purge_queue($cb);
};

sub get_job {
    my ($cb, $url) = @_;
    $url ||= $queue_url;
    my $req = HTTP::Request->new(GET => $url);
    my $res = $cb->($req);
    $res;
};

sub create_job {
    my ($cb, $job, $url) = @_;
    $url ||= $queue_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->header('Content-Type' => 'application/json');
    $req->header('X-presque-workerid' => $worker_id);
    $req->content(JSON::encode_json($job));
    my $res = $cb->($req);
    $res;
};

sub purge_queue {
    my ($cb, $url ) = @_;
    $url ||= $queue_url;
    my $req = HTTP::Request->new(DELETE => $url);
    my $res = $cb->($req);
    $res;
}

done_testing();
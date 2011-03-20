use strict;
use warnings;

use Test::More;
use Plack::Test;

use JSON;
use HTTP::Request;
use presque;

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
my @queues			 = [ "presque_test_1", "presque_test_2", "presque_test_3" ];

test_psgi $app, sub {
    my $cb = shift;
    my ($req, $res);
    my $content;


    # create a new job
    my $job = {foo => "bar-q1"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_1");
	$job = {foo => "bar-q1-1"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_1");

	$job = {foo => "bar-q2"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_2");
	$job = {foo => "bar-q3"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_3");

	# create virtual
	$res = create_virtual_queue($cb, $control_url, "seq");
	ok $res->is_success, 'virtual queue created';

	# change virtual queue
	$res = change_virtual_queue($cb, $control_url, "seq");
	ok $res->is_success, 'virtual queue created';

    # purge queue
    $res = purge_queue($cb);
    is $res->code, 204, 'queue purge';

    # check purged
};

sub get_stats_from_queue {
    my ($cb, $url) = @_;
    $url ||= $job_url;
    my $req = HTTP::Request->new(GET => $url);
    ok my $res = $cb->($req), 'get info on an empty queue';
    $res;
}

sub get_job {
    my ($cb, $url) = @_;
    $url ||= $queue_url;
    my $req = HTTP::Request->new(GET => $url);
    ok my $res = $cb->($req), 'first request done';
    $res;
}

sub get_jobs {
    my ($cb, $url) = @_;
    $url ||= $queue_batch_url;
    my $req = HTTP::Request->new(GET => $url);
    $req->header('X-presque-workerid' => $worker_id);
    ok my $res = $cb->($req);
    $res;
}

sub create_job {
    my ($cb, $job, $url) = @_;
    $url ||= $queue_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->header('Content-Type' => 'application/json');
    $req->header('X-presque-workerid' => $worker_id);
    $req->content(JSON::encode_json($job));
    ok my $res = $cb->($req);
    $res;
}

sub create_jobs {
    my ($cb, $jobs, $url) = @_;
    $url ||= $queue_batch_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->header('Content-Type' => 'application/json');
    $req->content(JSON::encode_json({jobs => $jobs}));
    ok my $res = $cb->($req);
    $res;
}

sub failed_job {
    my ($cb, $url) = @_;
    $url ||= $queue_url;
    my $req = HTTP::Request->new(PUT => $url);
    $req->header('Content-Type' => 'application/json');
    $req->content(JSON::encode_json({foo => 1}));
    ok my $res = $cb->($req), 'store a failed job';
    $res;
}

sub control_queue {
    my ($cb, $url) = @_;
    $url ||= $control_url;
    my $req = HTTP::Request->new(GET => $url);
    ok my $res = $cb->($req);
    $res;
}

sub change_queue_status {
    my ($cb, $status, $url) = @_;
    $url ||= $control_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->content(JSON::encode_json({status => $status}));
    ok my $res = $cb->($req);
    $res;
}

sub create_virtual_queue {
    my ($cb, $url, $distribution ) = @_;
    $url ||= $control_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->content(JSON::encode_json({type => "virtual", distribution => $distribution, 
    	queues => @queues }));
    ok my $res = $cb->($req);
    $res;
}

sub change_virtual_queue {
    my ($cb, $url, $distribution ) = @_;
    $url ||= $control_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->content(JSON::encode_json({type => "virtual", distribution => $distribution, 
    	queues => @queues }));
    ok my $res = $cb->($req);
    $res;
}

sub queue_status {
    my ($cb, $url) = @_;
    $url ||= $status_url;
    my $req = HTTP::Request->new(GET => $url);
    ok my $res = $cb->($req);
    $res;
}

sub workers_stats {
    my ($cb, $url ) = @_;
    $url ||= $worker_stats_url;
    my $req = HTTP::Request->new(GET => $url);
    ok my $res = $cb->($req);
    $res;
}

sub reg_worker {
    my ($cb) = @_;
    my $req = HTTP::Request->new(POST => $worker_url . "$queue");
    $req->header('Content-Type'       => 'application/json');
    $req->header('X-presque-workerid' => $worker_id);
    ok my $res = $cb->($req);
    $res;
}

sub unreg_worker {
    my ($cb) = @_;
    my $req = HTTP::Request->new(DELETE => $worker_url . "$queue");
    $req->header('X-presque-workerid' => $worker_id);
    ok my $res = $cb->($req);
    $res;
}

sub purge_queue {
    my ($cb, $url) = @_;
    $url ||= $queue_url;
    my $req = HTTP::Request->new(DELETE => $url);
    ok my $res = $cb->($req);
    $res;
}

done_testing;


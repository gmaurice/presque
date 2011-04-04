use strict;
use warnings;

use Test::More;
use Plack::Test;

use JSON;
use HTTP::Request;
use presque;
use YAML::Syck;

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
my $queues			 = [ "presque_test_1", "presque_test_2", "presque_test_3" ];
my $change_queues	 = [ "presque_test_1___change", "presque_test_2___change" ];

test_psgi $app, sub {
    my $cb = shift;
    my ($req, $res);
    my $content;


    # create new jobs
    my $job = {foo => "bar-q1"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_1");
	$job = {foo => "bar-q1-1"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_1");
 	$job = {foo => "bar-q2"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_2");
	$job = {foo => "bar-q3"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_3");
	$job = {foo => "bar-q3-1"};
    $res = create_job($cb, $job, "http://localhost/q/presque_test_3");

	# create virtual
	$res = create_virtual_queue($cb, $control_url);
	ok $res->is_success, 'virtual queue set';	
	
	# conflict creation
	$res = create_virtual_queue($cb, "http://localhost/control/presque_test_1");
	ok ! $res->is_success, 'virtual queue creation with conflict';

	# get job
	$res = get_job($cb, "http://localhost/q/$queue");
	is_deeply JSON::decode_json $res->content,
      { foo => "bar-q1" },
      'get job 1 by virtual queue ok';

 	$res = get_job($cb, "http://localhost/q/$queue");
 	is_deeply JSON::decode_json $res->content,
       { foo => "bar-q2" },
       'get job 2 by virtual queue ok';

	$res = get_job($cb, "http://localhost/q/$queue");
	is_deeply JSON::decode_json $res->content,
      { foo => "bar-q3" },
      'get job 3 by virtual queue ok';
	
	$res = get_job($cb, "http://localhost/q/$queue");
	is_deeply JSON::decode_json $res->content,
      { foo => "bar-q1-1" },
      'get job 1-1 by virtual queue ok';

	$res = get_job($cb, "http://localhost/q/$queue");
	is_deeply JSON::decode_json $res->content,
      { foo => "bar-q3-1" },
      'get job 3-1 by virtual queue ok';


    # clear virtual queue
	$res = set_virtual_queue($cb, $control_url, (@$queues, @$change_queues) );
    is_deeply JSON::decode_json $res->content,
      { queue => $queue, response => 'virtual queue set' },
      "clear queues from $queue";

    # status after adding queues
    $res = queue_status($cb);
    is_deeply JSON::decode_json $res->content,
      { 
        queue => 'presque_test', 
        type => 'virtual',
        size => 5,
        queues => [ "presque_test_1", "presque_test_2", "presque_test_3", "presque_test_1___change", "presque_test_2___change" ] }, 
        'virtual queue status after add';
    
    # clear virtual queue
	$res = destroy_virtual_queue($cb, $control_url, "destroy");
    is_deeply JSON::decode_json $res->content,
      { queue => $queue, response => 'virtual queue destroyed' },
      "clear queues from $queue";
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
    ok my $res = $cb->($req), 'get_job request done';
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
    my ($cb, $url) = @_;
    $url ||= $control_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->content(JSON::encode_json({ type => 'virtual', queues => $queues, action => 'set' }));
    ok my $res = $cb->($req);
    $res;
}

sub destroy_virtual_queue {
    my ($cb, $url, $action) = @_;
    $url ||= $control_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->content(JSON::encode_json({ type => 'virtual', action => "destroy" }));
    ok my $res = $cb->($req);
    $res;
}

sub set_virtual_queue {
    my ($cb, $url, @qs) = @_;
    $url ||= $control_url;
    my $req = HTTP::Request->new(POST => $url);
    $req->content(JSON::encode_json({ type => 'virtual', queues => \@qs, action => 'set' }));
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


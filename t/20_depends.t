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
my $other_queue      = "presque_test_other";
my $worker_id        = "worker_foo";
my $queue_url        = "http://localhost/q/$queue";
my $other_queue_url  = "http://localhost/q/$other_queue";
my $queue_batch_url  = "http://localhost/qb/$queue";
my $job_url          = "http://localhost/j/$queue";
my $other_job_url    = "http://localhost/j/$other_queue";
my $status_url       = "http://localhost/status/$queue";
my $worker_stats_url = "http://localhost/w/?queue_name=$queue";
my $worker_url       = "http://localhost/w/";
my $control_url      = "http://localhost/control/$queue";

test_psgi $app, sub {
    my $cb = shift;
    my ($req, $res);
    my $content;

    # create a new job with depends
    my $job = {foo => "bar"};
    $res = create_job($cb, $job, $queue_url."?depends=UNIQ_a,$other_queue:UNIQ_b");
    ok $res->is_success, 'new job with depends inserted';

    # info about a queue
     $res = get_stats_from_queue($cb);
     is_deeply JSON::decode_json($res->content),
       { job_count     => 0,
         job_failed    => 0,
         job_processed => 0,
         queue_name    => $queue,
       },
       'valid jobs info after jobs + depends';

    # no job to do now
    $res = get_job($cb);
    ok !$res->is_success, 'no job';

    create_job($cb, { job => "a" } , $queue_url. "?uniq=UNIQ_a");
    $res = get_job($cb);
    is_deeply JSON::decode_json($res->content), { job => "a" }, 'got job UNIQ_a';

    # info about a queue
     $res = get_stats_from_queue($cb);
     is_deeply JSON::decode_json($res->content),
       { job_count     => 0,
         job_failed    => 0,
         job_processed => 1,
         queue_name    => $queue,
       },
       'valid jobs info after get_job UNIQ_a';
       
    $res = get_job($cb);
    ok !$res->is_success, 'no job';

    create_job($cb, { job => "b" }, $other_queue_url. "?uniq=UNIQ_b");
    $res = get_job($cb, $other_queue_url);
    is_deeply JSON::decode_json($res->content), { job => "b" }, 'got job UNIQ_b';

    # info about a queue
    $res = get_stats_from_queue($cb, $other_job_url);
     is_deeply JSON::decode_json($res->content),
       { job_count     => 0,
         job_failed    => 0,
         job_processed => 1,
         queue_name    => $other_queue,
       },
       'valid jobs info after get_job UNIQ_b';
       
    $res = get_job($cb);
    is_deeply JSON::decode_json($res->content), $job, 'got job with depends';

    # info about a queue
    $res = get_stats_from_queue($cb, $job_url);
     is_deeply JSON::decode_json($res->content),
       { job_count     => 0,
         job_failed    => 0,
         job_processed => 2,
         queue_name    => $queue,
       },
       'valid jobs info after get_job UNIQ_b';

    # control queue
    $res = control_queue($cb);
    is_deeply JSON::decode_json($res->content),
      { status => 1,
        queue  => 'presque_test'
      },
      'queue is open';
      
     purge_queue($cb, $queue_url);
     purge_queue($cb, $other_queue_url);

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

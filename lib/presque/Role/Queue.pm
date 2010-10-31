package presque::Role::Queue;

use Moose::Role;

sub new_queue {
    my ($self, $queue_name, $lkey) = @_;
    $self->application->redis->sadd('QUEUESET', $queue_name);
    my $ckey = $self->_queue_stat($queue_name);
    $self->application->redis->set($ckey, 1);
}

sub push_job {
    my ($self, $queue_name, $lkey, $key, $delayed) = @_;
    my ($method, @args) = ('rpush', $lkey, $key);

    if ($delayed) {
        $method = 'zadd';
        @args = ($queue_name . ':delayed', $delayed, $key);
    }

    $self->application->redis->$method(@args,);
}

sub release_waiting_jobs {
    my ($self, $queue_name, $push_in_queue) = @_;

    $self->application->redis->keys(
        "deps:$queue_name:*",
        sub {
            my $k = shift;
            my @keys = @$k;
            my $nb_jobs = scalar @keys;
            for my $key (@keys){
                $key =~ /deps:(.+)/ ;
                $key = $1;
                $self->push_job($queue_name, $self->_queue($queue_name), $key) if $push_in_queue;
                $self->application->redis->del($key) unless $push_in_queue;
                $self->application->redis->smembers(			
                    $self->_deps_queue_uuid($key),
                    sub {
                        my ($deps_uniqs) = @_;
                        my $dep_uniq;
                        for $dep_uniq (@$deps_uniqs){        	
                            $self->application->redis->srem(
                                $self->_deps_queue_uniq_revert($dep_uniq),
                                $key
                            );
                        }
                        $self->application->redis->del(
                            $self->_deps_queue_uuid($key)
                        );
                    }
                );
            }
            $self->finish({ action => "release", jobs_to_release => $nb_jobs });
        }
    );
}



1;

package presque::JobQueueHandler;

use Moose;
extends 'Tatsumaki::Handler';
__PACKAGE__->asynchronous(1);

sub get {
    my ( $self, $queue_name ) = @_;
    my $key = $queue_name . ':queue';
    $self->application->redis->lrange(
        $key, 0, 9,
        sub {
            my $jobs = shift;
            $self->application->redis->llen(
                $key,
                sub {
                    my $size = shift;
                    my $lkey = $queue_name . '*';
                    $self->application->redis->keys(
                        $lkey,
                        sub {
                            my $total = shift;
                            $self->render(
                                'job.html',
                                {
                                    queue      => $queue_name,
                                    jobs       => $jobs,
                                    job_count  => $size,
                                    queue_size => scalar @$total
                                }
                            );
                        }
                    );
                }
            );
        }
    );
}

1;
__END__

=head1 NAME

presque::IndexHandler - a redis based message queue

=head1 DESCRIPTION

=head1 AUTHOR

franck cuny E<lt>franck@lumberjaph.netE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright 2010 by Linkfluence

L<http://linkfluence.net>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
